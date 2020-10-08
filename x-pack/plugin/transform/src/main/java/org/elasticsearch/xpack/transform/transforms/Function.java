/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Interface for transform functions (e.g. pivot)
 *
 * The function interface abstracts:
 *  - mapping deduction
 *  - data preview
 *  - validation
 *  - collection of changes (finding the minimal update)
 *  - querying the source index
 *  - processing search results in order to write them to dest
 *  - access to the cursor (for resilience and pausing a transform)
 */
public interface Function {

    /**
     * Change collector
     *
     * The purpose of the change collector is minimizing the update required for continuous transforms.
     *
     * The change collector is stateful, changes are stored inside. For scaling the change collector has a
     * cursor and can run in iterations.
     *
     * In a nutshell the algorithm works like this:
     * 1. check and collect what needs to be updated, but only up to the page size limit
     * 2. apply the collected changes as filter query and search/process them
     * 3. in case phase 1 could not collect all changes, move the collector cursor, collect changes and continue with step 2
     */
    public interface ChangeCollector {

        /**
         * Build the search query to gather the changes between 2 checkpoints.
         *
         * @param searchSourceBuilder a searchsource builder instance
         * @param position the position of the change collector
         * @param pageSize the pageSize configured by the function, used as upper boundary, a lower page size might be used
         * @return the searchSource, expanded with the relevant parts
         */
        SearchSourceBuilder buildChangesQuery(SearchSourceBuilder searchSourceBuilder, Map<String, Object> position, int pageSize);

        /**
         * Process the search response of the changes query and remember the changes.
         *
         * TODO: replace the boolean with a more descriptive enum.
         *
         * @param searchResponse the response after querying for changes
         * @return true in case of no more changed buckets, false in case changes buckets have been collected
         */
        boolean processSearchResponse(SearchResponse searchResponse);

        /**
         * Build the filter query to narrow the result set given the previously collected changes.
         *
         * TODO: it might be useful to have the full checkpoint data.
         *
         * @param lastCheckpointTimestamp the timestamp of the last checkpoint
         * @param nextcheckpointTimestamp the timestamp of the next (in progress) checkpoint
         * @return a filter query, null in case of no filter
         */
        QueryBuilder buildFilterQuery(long lastCheckpointTimestamp, long nextcheckpointTimestamp);

        /**
         * Clear the internal state to free up memory.
         */
        void clear();

        /**
         * Get the bucket position of the changes collector.
         *
         * @return the position, null in case the collector is exhausted
         */
        Map<String, Object> getBucketPosition();

        /**
         * Whether the collector optimizes change detection by narrowing the required query.
         *
         * @return true if the collector optimizes change detection
         */
        boolean isOptimized();
    }

    /**
     * Deduce mappings based on the input mappings and the known configuration.
     *
     * @param client a client instance for querying the source mappings
     * @param sourceConfig the source configuration
     * @param listener listener to take the deduced mapping
     */
    void deduceMappings(Client client, SourceConfig sourceConfig, ActionListener<Map<String, String>> listener);

    /**
     * Create a preview of the function.
     *
     * @param client a client instance for querying
     * @param headers headers to be used to query only for what the caller is allowed to
     * @param sourceConfig the source configuration
     * @param fieldTypeMap mapping of field types
     * @param numberOfRows number of rows to produce for the preview
     * @param listener listener that takes a list, where every entry corresponds to 1 row/doc in the preview
     */
    void preview(
        Client client,
        Map<String, String> headers,
        SourceConfig sourceConfig,
        Map<String, String> fieldTypeMap,
        int numberOfRows,
        ActionListener<List<Map<String, Object>>> listener
    );

    /**
     * Get the search query for querying for initial (first checkpoint) progress
     *
     * @param searchSourceBuilder a searchsource builder instance
     * @return the searchSource, expanded with the relevant parts
     */
    SearchSourceBuilder buildSearchQueryForInitialProgress(SearchSourceBuilder searchSourceBuilder);

    /**
     * Process the search response from progress search call and return progress information.
     *
     * @param response the search response
     * @param progressListener listener that takes the progress information as call back
     */
    void getInitialProgressFromResponse(SearchResponse response, ActionListener<TransformProgress> progressListener);

    /**
     * Validate the configuration.
     *
     * @param listener the result listener
     */
    void validateConfig(ActionListener<Boolean> listener);

    /**
     * Runtime validation by querying the source and checking if source and config fit.
     *
     * @param client a client instance for querying the source
     * @param sourceConfig the source configuration
     * @param listener the result listener
     */
    void validateQuery(Client client, SourceConfig sourceConfig, ActionListener<Boolean> listener);

    /**
     * Create a change collector instance and return it
     *
     * @param synchronizationField the field used for synchronizing (continuous mode)
     * @return a change collector instance
     */
    ChangeCollector buildChangeCollector(String synchronizationField);

    /**
     * Get the initial page size for this function.
     *
     * The page size is the main parameter for adjusting memory consumption. Memory consumption mainly depends on
     * the page size, the type of aggregations and the data. As the page size is the number of buckets we return
     * per page the page size is a multiplier for the costs of aggregating bucket.
     *
     * In future we might inspect the configuration and base the initial size on the aggregations used.
     *
     * @return the page size
     */
    int getInitialPageSize();

    /**
     * Whether this function - given its configuration - supports incremental bucket update used in continuous mode.
     *
     * If so, the indexer uses the change collector to update the continuous transform.
     *
     * TODO: simplify and remove this method if possible
     *
     * @return true if incremental bucket update is supported
     */
    boolean supportsIncrementalBucketUpdate();

    /**
     * Build the query for the next iteration
     *
     * @param searchSourceBuilder a searchsource builder instance
     * @param position current position (cursor/page)
     * @param pageSize the pageSize, defining how much data to request
     * @return the searchSource, expanded with the relevant parts
     */
    SearchSourceBuilder buildSearchQuery(SearchSourceBuilder searchSourceBuilder, Map<String, Object> position, int pageSize);

    /**
     * Process the search response and return a stream of index requests as well as the cursor.
     *
     * @param searchResponse the search response
     * @param destinationIndex the destination index
     * @param destinationPipeline the destination pipeline
     * @param fieldMappings field mappings for the destination
     * @param stats a stats object to record/collect stats
     * @return a tuple with the stream of index requests and the cursor
     */
    Tuple<Stream<IndexRequest>, Map<String, Object>> processSearchResponse(
        SearchResponse searchResponse,
        String destinationIndex,
        String destinationPipeline,
        Map<String, String> fieldMappings,
        TransformIndexerStats stats
    );
}
