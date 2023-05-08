#include <iostream>
#include <vector>
#include <map>
#include <algorithm>
#include <thread>

using namespace std;

// USER-DEFINED mapping function -- This is run on the entirety of the chunk
vector<pair<char, int>> mapper(const vector<char> &chunk)
{
    vector<pair<char, int>> result;
    for (int i = 0; i < chunk.size(); ++i)
    {
        result.push_back(make_pair(chunk[i], 1));
    }
    return result;
}

// USER-DEFINED reducing function -- This is run on shuffled and sorted sub-chunk
int reducer(char key, const vector<int> &values)
{
    int result = 0;
    for (int i = 0; i < values.size(); ++i)
    {
        result += values[i];
    }
    return result;
}

// Perform the MapReduce operation
vector<int> map_reduce(const vector<char> &data, int num_workers)
{

    // ============== PRE-PROCESSING ==============

    // Split the data into chunks for mapping
    // Each chunk is handed to one worker thread
    int chunk_size = (data.size()) / num_workers;
    vector<vector<char>> chunks;
    for (int i = 0; i < num_workers; ++i)
    {
        int start = i * chunk_size;
        int end;
        if (i == num_workers - 1)
        {
            // If it's the last worker, include any remaining elements in the last chunk
            end = data.size();
        }
        else
        {
            // Calculate the end index of the current chunk
            end = start + chunk_size;
        }

        // Create a new vector from the subrange of the data vector
        vector<char> chunk(data.begin() + start, data.begin() + end);

        // Add the chunk to the vector of chunks
        chunks.push_back(chunk);
    }

    // ============== MAP PHASE ==============

    // Create a vector of vectors of size (num_workers)
    vector<vector<pair<char, int>>> mapped_data(num_workers);
    vector<thread> mapping_threads;

    for (int i = 0; i < num_workers; ++i)
    {
        // Create a function representing the mapping task for the current worker
        auto mapping_task = [&chunks, &mapped_data, i]()
        {
            // Retrieve the data chunk assigned to the current worker
            vector<char> &chunk = chunks[i];

            // Perform the mapping operation on the data chunk
            vector<pair<char, int>> mapped_result = mapper(chunk);

            // Store the mapped result in the corresponding index of the mapped_data vector
            mapped_data[i] = mapped_result;
        };

        // Create a thread with the mapping task and add it to the vector of threads
        mapping_threads.emplace_back(mapping_task);
    }

    // Wait for all mapping threads to complete
    for (auto &thread : mapping_threads)
    {
        thread.join();
    }

    // ============== INTERMEDIATE PHASE ==============

    // Flatten the mapped data
    vector<pair<char, int>> flattened_data;
    for (const auto &chunk_data : mapped_data)
    {
        flattened_data.insert(flattened_data.end(), chunk_data.begin(), chunk_data.end());
    }

    // Group the mapped data by key
    map<char, vector<int>> grouped_data;
    for (const auto &kvp : flattened_data)
    {
        grouped_data[kvp.first].push_back(kvp.second);
    }

    // ============== REDUCE PHASE ==============

    vector<int> reduced_data;
    vector<thread> reducing_threads;

    for (const auto &kvp : grouped_data)
    {
        // Create a function representing the reducing task for the current key-value pair
        auto reducing_task = [&reduced_data, &kvp]()
        {
            // Retrieve the key and values associated with the current key-value pair
            char key = kvp.first;
            const vector<int> &values = kvp.second;

            // Perform the reducing operation on the key and values
            int reduced_result = reducer(key, values);

            // Add the reduced result to the reduced_data vector
            reduced_data.push_back(reduced_result);
        };

        // Create a thread with the reducing task and add it to the vector of threads
        reducing_threads.emplace_back(reducing_task);
    }

    // Wait for all reducing threads to complete
    for (auto &thread : reducing_threads)
    {
        thread.join();
    }

    return reduced_data;
}

int main()
{
    // Sample input data
    vector<char> data = {'a', 'b', 'c', 'b', 'a', 'a', 'b', 'c'};
    int num_workers = 3;

    // Perform MapReduce operation
    vector<int> result = map_reduce(data, num_workers);

    for (const auto &value : result)
    {
        cout << value << " ";
    }
    cout << endl;

    return 0;
}
