/**
 * @file bfs_ff.cpp
 * @author Marco Costa
 * @brief The BFS search executable implemented with the FastFlow framework
 * @version 0.1
 * @date 2021-09-08
 */
#include <iostream>
#include <vector>
#include <unordered_set>
#include <algorithm>

#include "ff/ff.hpp"
#include "ff/parallel_for.hpp"

#include "utimer.cpp"
#include "graph.cpp"
#include "utils.cpp"
#include "config.hpp"

/**
 * @brief The BFS search using the FastFlow framework.
 * 
 * @param g the Graph to which execute the search
 * @param start_node the starting node
 * @param search_value the value to search 
 * @param n_workers the number of workers
 * @return int the occurrences found
 */
int ff_bfs(Graph *g, int start_node, int search_value, int n_workers)
{
    /* initialization of the data structures needed */
    vector<int> curr_frontier;
    vector<vector<int>> partial_new_frontier(n_workers);
    vector<int> partial_results(n_workers);
    vector<int> visited(g->n_nodes); /* used as vector<bool> (which is not concurrent) */ 

    ff::ParallelFor pfr = ff::ParallelFor(n_workers);

    /* routine of each worker */
    auto f = [&](const int i, const int thread_no)
    {
        int partial_occurrences = 0;

#ifdef DEBUG_PRINT
        printf("th %d: [%d, %d) {%d, %d}\n", thread_no, start, stop, curr_size, delta);
#endif
        auto curr_node = curr_frontier[i];

#ifdef DEBUG_PRINT
        printf("Thread %d: taking %d\n", thread_no, curr_node);
#endif

        if (g->nodes[curr_node]->value == search_value)
            partial_occurrences++;

        for (auto &val : g->nodes[curr_node]->adj)
        {
            if (visited[val] == 0) /* if not visited before */
            {
                partial_new_frontier[thread_no].push_back(val);
                visited[val] = 1;
            }
        }

        partial_results[thread_no] += partial_occurrences;
    };

    curr_frontier.push_back(start_node);
    visited[start_node] = 1;

    unordered_set<int> merging_set;
    while (!curr_frontier.empty())
    {
        // dynamic scheduling performs also good but introduces too much overhead on a low number of nodes
        // round robin seems to perform the same on an high number of nodes but without overhead
        // using parallel_for_thid in order to give access to each worker to its reserved structures
        pfr.parallel_for_thid(0, curr_frontier.size(), 1, -CHUNK_SIZE, f);

        for (size_t i = 0; i < partial_new_frontier.size(); i++)
        {
            merging_set.insert(partial_new_frontier[i].begin(), partial_new_frontier[i].end());
            partial_new_frontier[i].clear();
        }

        /* merging phase with sorting */
        curr_frontier.assign(merging_set.begin(), merging_set.end());
        merging_set.clear();
        sort(curr_frontier.begin(), curr_frontier.end());
    }

    /* local reduce */
    int total_occurrences = 0;
    for (auto &val : partial_results)
        total_occurrences += val;

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
        utimer tff("tff");
        occ = ff_bfs(g, start_node, search_value, n_workers);
    }
    std::cout << "Occurrences: " << occ << endl;
    return 0;
}
#endif