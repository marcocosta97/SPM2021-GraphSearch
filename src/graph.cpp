/**
 * @file graph.cpp
 * @author Marco Costa
 * @brief The file that contains the graph and node classes common to the different search executables
 * @version 0.1
 * @date 2021-09-08
 * 
 */
#ifndef GRAPH_CPP
#define GRAPH_CPP

#include <iostream>
#include <vector>
#include <set>
#include <random>

using namespace std;

typedef unsigned int uint;
static uint g_seed = 1234;

inline int fastrand()
{
    g_seed = (214013 * g_seed + 2531011);
    return (g_seed >> 16) & 0x7FFF;
}

/**
 * @brief Represents a node object with its value associated and 
 *        vector of adjacencies
 */
class node
{
public:
    short value;
    vector<uint> adj;
};

/**
 * @brief Graph class which represents one graph of nodes of class
 *        `node`
 */
class Graph
{
private:
    void clear_duplicates(vector<set<int>> dup_set, short max_value);
public:
    uint n_nodes;
    vector<node *> nodes;
    Graph(uint n_nodes);
    ~Graph();
    void add_edge(uint n1, uint n2);
    void set_value(uint node_i, short value);
    void print_dot();

    static Graph *generate_graph(uint n_nodes, int seed, short max_value, int percent);
    static Graph *generate_graph_fast(uint n_nodes, uint n_edges, int seed, short max_value);

    static void save_to_file(Graph *g, string filename);
    static Graph *load_from_file(string filename);
};

Graph::Graph(uint n_nodes)
{
    this->n_nodes = n_nodes;
    nodes.reserve(n_nodes);

    for (uint i = 0; i < n_nodes; i++)
        nodes[i] = new node();
}

Graph::~Graph()
{
    for (uint i = 0; i < n_nodes; i++)
    {
        nodes[i]->adj.clear();
        delete nodes[i];
    }
    nodes.clear();
}

inline void Graph::add_edge(uint n1, uint n2)
{
    nodes[n1]->adj.push_back(n2);
}

inline void Graph::set_value(uint node_i, short value)
{
    nodes[node_i]->value = value;
}

void Graph::clear_duplicates(vector<set<int>> dup_set, short max_value)
{
    for (uint i = 0; i < n_nodes; i++)
    {
        this->nodes[i]->adj.insert(this->nodes[i]->adj.end(), dup_set[i].begin(), dup_set[i].end());
        this->set_value(i, (rand() % max_value) + 1);
    }
}

/* Return a random integer between 0 and k-1 inclusive. */
inline int ran(int k)
{
    return rand() % k;
}

void swap(int *a, int *b)
{
    int temp;

    temp = *a;
    *a = *b;
    *b = temp;
}

Graph *Graph::generate_graph_fast(uint n_nodes, uint n_edges, int seed, short max_value)
{
    int i, j;
    Graph *g = new Graph(n_nodes);

    srand(seed);
    vector<set<int>> temp(n_nodes);

    for (uint count = 0; count < n_edges;)
    {
        if ((i = ran(n_nodes)) == (j = ran(n_nodes)))
            continue;
        if (i > j)
            swap(&i, &j);

        if (temp[i].insert(j).second)
            count++;
    }

    g->clear_duplicates(temp, max_value);

    return g;
}

Graph *Graph::generate_graph(uint n_nodes, int seed, short max_value, int percent)
{
    Graph *g = new Graph(n_nodes);
    uint old_seed = g_seed;
    g_seed = seed;

    unsigned long rr;

    for (uint i = 0; i < n_nodes; i++)
    {
        for (uint j = (i + 1); j < n_nodes; j++)
        {
            rr = fastrand();
            if ((int) (rr % 100) < percent)
                g->add_edge(i, j);
        }

        g->set_value(i, (fastrand() % max_value) + 1);
    }

    g_seed = old_seed;
    return g;
}

void Graph::print_dot()
{
    printf("digraph {\n");
    for (uint i = 0; i < n_nodes; i++)
    {
        for (auto &curr : this->nodes[i]->adj)
            printf("  %d -> %d;\n", i, curr);
    }
    printf("}\n");
}

#endif