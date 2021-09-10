/**
 * @file bfs_seq.cpp
 * @author Marco Costa
 * @brief Sequential implementation of the BFS search
 * @version 0.1
 * @date 2021-09-08 
 */
#include <iostream>
#include <queue>

#include "graph.cpp"
#include "utimer.cpp"
#include "utils.cpp"
#include "config.hpp"

/**
 * @brief The classical BFS sequential search
 * 
 * @param g the graph where to perform the search
 * @param start_node the starting node
 * @param search_value the value to search
 * @return int the number of occurrences found
 */
int sequential_bfs(Graph *g, int start_node, int search_value)
{
    int occ = 0;

    queue<int> q; // way faster than queue<unsigned int> why? I don't really know
    vector<bool> visited(g->n_nodes); 

    q.push(start_node);
    visited[start_node] = true;

    while (!q.empty())
    {
        int curr = q.front();
        q.pop();

#ifdef DEBUG_PRINT
        printf("(%d, %d, %d), ", curr, g->nodes[curr]->value, d[curr]);
#endif

        if (g->nodes[curr]->value == search_value)
            occ++;

        for (auto &val : g->nodes[curr]->adj)
        {
            if (!visited[val]) /* if has not been visited before */
            {
                q.push(val);
                visited[val] = true;
            }
        }
    }

    return occ;
}

#ifndef TEST_CPP
int main(int argc, char *argv[])
{
    if (argc < 2)
    {
        printf("Usage: %s n_nodes --start [start_node] --search [search_value] \
        --max [max_value] --seed [seed_value]\n",
               argv[0]);
        exit(-1);
    }
    int n_nodes = atoi(argv[1]);

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
        utimer tseq("tseq");
        occ = sequential_bfs(g, start_node, search_value);
    }
    std::cout << "Occurrences: " << occ << endl;

    delete g;
    return 0;
}
#endif