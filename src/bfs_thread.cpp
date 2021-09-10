/**
 * @file bfs_thread.cpp
 * @author Marco Costa
 * @brief The plain C++ BFS parallel search
 * @version 0.1
 * @date 2021-09-08
 */
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <unistd.h>
#include <algorithm>
#include <unordered_set>

#include "graph.cpp"
#include "utimer.cpp"
#include "utils.cpp"
#include "config.hpp"

/**
 * @brief Class which implements a multiple workers with multiple generations barrier
 */
class Barrier
{
public:
    explicit Barrier(std::size_t i_count) : m_threshold(i_count),
                                           m_count(i_count),
                                           m_generation(0)
    {
    }

    /**
     * @brief Puts the master on wait, waiting to be woken up by the workers
     */
    void MasterWait()
    {
        unique_lock<std::mutex> m_lock{m_mutex};
        m_cond.wait(m_lock, [this]
                    { return stop_master; });
        stop_master = false;
    }

    /**
     * @brief Starts the workers which should be on wait
     */
    void StartWorkers()
    {
        unique_lock<std::mutex> w_lock{w_mutex};
        m_generation++;
        w_cond.notify_all();
    }

    /**
     * @brief Puts a worker on wait, if that worker is the last to be wake it wakes up
     *        the master thread before putting himself on wait
     */
    void WorkerWait()
    {
        unique_lock<std::mutex> w_lock{w_mutex};
        auto l_gen = m_generation;
        if (!--m_count)
        {
            m_count = m_threshold;
            stop_master = true;
            m_cond.notify_one();
        }
        w_cond.wait(w_lock, [this, l_gen]
                    { return l_gen != m_generation; }); /* needed to avoid spurious wakeups */ 
    }

private:
    std::mutex w_mutex; /* the workers mutex */
    std::mutex m_mutex; /* the master mutex */
    std::condition_variable w_cond; /* the workers condition variable */
    std::condition_variable m_cond; /* the master condition variable */
    std::size_t m_threshold; /* the total number of workers */
    std::size_t m_count; /* the current number of workers not in wait */
    std::size_t m_generation; /* the current generation */
    bool stop_master = false;
};

/**
 * @brief The BFS search using the plain C++.
 * 
 * @param g the Graph to which execute the search
 * @param start_node the starting node
 * @param search_value the value to search 
 * @param n_workers the number of workers
 * @return int the occurrences found
 */
int parallel_bfs(Graph *g, int start_node, int search_value, int n_workers)
{
    vector<int> curr_frontier;
    vector<vector<int>> partial_new_frontier(n_workers);
    vector<int> partial_results(n_workers);
    
    Barrier *barrier = new Barrier(n_workers);

    vector<int> visited(g->n_nodes);
    bool game_over = false;

    /* worker routine, note the worker exits this function only when the BFS is over */
    auto f = [&](int thread_no, int chunk_size)
    {
        int partial_occurrences = 0;
        while (!game_over)
        {
            /* computing the current chunks indices */
            size_t curr_size = curr_frontier.size();
            int number_of_chunks = (curr_size / chunk_size);
            bool extra_chunk = ((thread_no == 0) && ((int)(curr_size % chunk_size) > 0)) ? true : false;

            auto f_node = [&](auto curr_node)
            {
                if (g->nodes[curr_node]->value == search_value)
                    partial_occurrences++;

                for (auto &val : g->nodes[curr_node]->adj)
                {
                    if (visited[val] == 0)
                    {
                        partial_new_frontier[thread_no].push_back(val);
                        visited[val] = 1;
                    }
                }
            };

            for (auto i = thread_no; i < number_of_chunks; i += n_workers)
            {
                auto start = i * chunk_size;
                auto stop = start + chunk_size;

#ifdef DEBUG_PRINT
                printf("th %d: [%d, %d)\n", thread_no, start, stop);
#endif

                for (int j = start; j < stop; j++)
                {
                    auto curr_node = curr_frontier[j];
#ifdef DEBUG_PRINT
                    printf("Thread %d: taking %d\n", thread_no, curr_node);
#endif
                    f_node(curr_node);
                }
            }

            /* not even, there is an extra chunk to be computed */
            if (extra_chunk)
            {
                for (size_t i = number_of_chunks * chunk_size; i < curr_size; i++)
                {
                    auto curr_node = curr_frontier[i];
                    f_node(curr_node);
                }
            }

            /* job done, go on wait */
            barrier->WorkerWait();
        }

        partial_results[thread_no] = partial_occurrences;
    };

    curr_frontier.push_back(start_node);
    visited[start_node] = 1;
    bool first_iteration = true;
    vector<thread *> thread_ids(n_workers);
    for (int i = 0; i < n_workers; i++)
        thread_ids[i] = new thread(f, i, CHUNK_SIZE);

    unordered_set<int> merging_set;
    while (!curr_frontier.empty())
    {
        if (!first_iteration) // already started
            barrier->StartWorkers();
        else
            first_iteration = false;

        /* putting itself on wait */
        barrier->MasterWait();
        
        /* merging phase */
        for (size_t i = 0; i < partial_new_frontier.size(); i++)
        {
            merging_set.insert(partial_new_frontier[i].begin(), partial_new_frontier[i].end());
            partial_new_frontier[i].clear();
        }

        curr_frontier.assign(merging_set.begin(), merging_set.end());

        merging_set.clear();
        sort(curr_frontier.begin(), curr_frontier.end());
    }

    /* stopping the workers, and waking them up in case someone is on wait */ 
    game_over = 1;
    barrier->StartWorkers();

    /* local reduce */
    int total_occurrences = 0;
    for (int i = 0; i < n_workers; i++)
    {
        thread_ids[i]->join();
        total_occurrences += partial_results[i];
        delete thread_ids[i];
    }

    delete barrier;

    return total_occurrences;
}

// static partitioning version of the parallel BFS, used only for test
int __parallel_bfs_static(Graph *g, int start_node, int search_value, int n_workers)
{
    vector<int> curr_frontier;
    vector<vector<int>> partial_new_frontier(n_workers);
    vector<int> partial_results(n_workers);
    bool game_over = false;
    Barrier *barrier = new Barrier(n_workers);

    vector<int> d(g->n_nodes);

    auto f = [&](int thread_no)
    {
        int partial_occurrences = 0;
        while (!game_over)
        {
            size_t curr_size = curr_frontier.size();
            if ((int)curr_size > thread_no) // temporary solutions waiting for chunks
            {
                // compute range
                /* better to use chunks? sets return an ordered vector? */
                auto delta = (n_workers < (int)curr_size) ? curr_size / n_workers : 1;
                auto start = thread_no * delta;
                auto stop = (thread_no == (n_workers - 1)) ? curr_size : ((thread_no + 1) * delta);

                if (start == stop)
                    stop++;

#ifdef DEBUG_PRINT
                printf("th %d: [%d, %d) {%d, %d}\n", thread_no, start, stop, curr_size, delta);
#endif

                for (unsigned int i = start; i < stop; i++)
                {
                    auto curr_node = curr_frontier[i];

#ifdef DEBUG_PRINT
                    printf("Thread %d: taking %d\n", thread_no, curr_node);
#endif

                    if (g->nodes[curr_node]->value == search_value)
                        partial_occurrences++;

                    for (auto &val : g->nodes[curr_node]->adj)
                    {
                        if (d[val] == 0)
                        {
                            partial_new_frontier[thread_no].push_back(val);
                            d[val] = 1;
                        }
                    }
                }
            }

            barrier->WorkerWait();
        }

        partial_results[thread_no] = partial_occurrences;
    };

    curr_frontier.push_back(start_node);
    d[start_node] = 1;
    int it = 0;
    vector<thread *> thread_ids(n_workers);
    for (int i = 0; i < n_workers; i++)
        thread_ids[i] = new thread(f, i);

    unordered_set<int> merging_set;
    while (!curr_frontier.empty())
    {
        // split the frontier to the workers
        // wait the workers (barrier)
        // union of the workers partial frontiers
        if (it > 0)
            barrier->StartWorkers();
        barrier->MasterWait();
        // reduce the results
        it++;
        for (size_t i = 0; i < partial_new_frontier.size(); i++)
        {
            merging_set.insert(partial_new_frontier[i].begin(), partial_new_frontier[i].end());
            partial_new_frontier[i].clear();
        }

        curr_frontier.assign(merging_set.begin(), merging_set.end()); // si può cambiare come per sopra??
        merging_set.clear();
    }

    game_over = 1;
    barrier->StartWorkers();

    int total_occurrences = 0;
    for (int i = 0; i < n_workers; i++)
    {
        thread_ids[i]->join();
        total_occurrences += partial_results[i];
        delete thread_ids[i];
    }

    delete barrier;

    return total_occurrences;
}

#ifndef TEST_CPP
int main(int argc, char *argv[])
{
    if (argc < 3)
    {
        printf("Usage: %s n_nodes n_threads --start [start_node] --search [search_value] \
        --max [max_value] --seed [seed_value]\n",
               argv[0]);
        exit(-1);
    }
    int n_nodes = atoi(argv[1]);
    int n_workers = atoi(argv[2]);

    int start_node = (cmdOptionExists(argv, argv + argc, "--start")) ? atoi(getCmdOption(argv, argv + argc, "--start"))
                                                                     : default_start_node;
    int search_value = (cmdOptionExists(argv, argv + argc, "--search")) ? atoi(getCmdOption(argv, argv + argc, "--search"))
                                                                        : default_search_value;
    int max = (cmdOptionExists(argv, argv + argc, "--max")) ? atoi(getCmdOption(argv, argv + argc, "--max"))
                                                            : default_max_value;
    int seed = (cmdOptionExists(argv, argv + argc, "--seed")) ? atoi(getCmdOption(argv, argv + argc, "--seed"))
                                                              : default_seed_value;
    int percent = (cmdOptionExists(argv, argv + argc, "--percent")) ? atoi(getCmdOption(argv, argv + argc, "--percent"))
                                                              : default_percent_value;

    Graph *g = Graph::generate_graph(n_nodes, seed, max, percent);
    int occ = -1;
    {
        utimer tpar("tpar");
        occ = parallel_bfs(g, start_node, search_value, n_workers);
    }
    std::cout << "Occurrences: " << occ << endl;


    delete g;
    return 0;
}
#endif
