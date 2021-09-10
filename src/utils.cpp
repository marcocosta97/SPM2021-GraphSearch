/**
 * @file utils.cpp
 * @author Marco Costa
 * @brief Utils for the command line parsing
 * @version 0.1
 * @date 2021-09-08
 */
#ifndef UTILS_CPP
#define UTILS_CPP

#include <iostream>
#include <algorithm>

char* getCmdOption(char ** begin, char ** end, const std::string & option)
{
    char ** itr = std::find(begin, end, option);
    if (itr != end && ++itr != end)
        return *itr;
    
    return 0;
}

bool cmdOptionExists(char** begin, char** end, const std::string& option)
{
    return std::find(begin, end, option) != end;
}

#endif