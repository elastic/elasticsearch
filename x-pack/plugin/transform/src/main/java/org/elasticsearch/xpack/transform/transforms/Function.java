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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.transform.transforms.SourceConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;

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
     * In a nutshell it uses queries to
     * 1. check and collect what needs to be updated
     * 2. apply the collected changes as filter query.
     */
    public interface ChangeCollector {

        boolean collectChanges(SearchResponse searchResponse);

        QueryBuilder filterByChanges(long lastCheckpointTimestamp, long nextcheckpointTimestamp);

        SearchSourceBuilder buildChangesQuery(SearchSourceBuilder sourceBuilder, Map<String, Object> position, int pageSize);

        void clear();

        Map<String, Object> getBucketPosition();
    }

    /**
     * Deduce mappings based on the input mappings and the known configuration.
     *
     * @param client a client instance for querying the source mappings
     * @param sourceConfig the source configuration
     * @param listener listener to take the deduced mapping
     */
    void deduceMappings(Client client, SourceConfig sourceConfig, ActionListener<Map<String, String>> listener);

    void preview(
        Client client,
        Map<String, String> headers,
        SourceConfig sourceConfig,
        Map<String, String> fieldTypeMap,
        int numberOfBuckets,
        ActionListener<List<Map<String, Object>>> listener
    );

    /**
     * Validate configuration.
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
     * @param builder a searchsource builder instance
     * @param position current position (cursor/page)
     * @param pageSize the pageSize, defining how much data to request
     * @return the searchSource, expanded with the relevant parts
     */
    SearchSourceBuilder buildSearchQuery(SearchSourceBuilder builder, Map<String, Object> position, int pageSize);

    /**
     * Process the search response and return a stream of index requests.
     *
     * @param searchResponse the search response
     * @param destinationIndex the destination index
     * @param destinationPipeline the destination pipeline
     * @param fieldMappings field mappings for the destination
     * @param stats a stats object to record/collect stats
     * @return stream of index requests
     */
    Stream<IndexRequest> processSearchResponse(
        SearchResponse searchResponse,
        String destinationIndex,
        String destinationPipeline,
        Map<String, String> fieldMappings,
        TransformIndexerStats stats
    );

    /**
     * Get the cursor given the search response.
     *
     * TODO: we might want to merge this with processSearchResponse
     *
     * @param searchResponse the search response
     * @return the cursor
     */
    Map<String, Object> getAfterKey(SearchResponse searchResponse);
}
