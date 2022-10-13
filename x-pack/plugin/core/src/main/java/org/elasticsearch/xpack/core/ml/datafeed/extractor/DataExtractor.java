/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed.extractor;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public interface DataExtractor {

    record Result(SearchInterval searchInterval, Optional<InputStream> data) {}

    /**
     * @return {@code true} if the search has not finished yet, or {@code false} otherwise
     */
    boolean hasNext();

    /**
     * Returns the next available extracted data. Note that it is possible for the
     * extracted data to be empty the last time this method can be called.
     * @return a result with the search interval and an optional input stream with the next available extracted data
     * @throws IOException if an error occurs while extracting the data
     */
    Result next() throws IOException;

    /**
     * @return {@code true} if the extractor has been cancelled, or {@code false} otherwise
     */
    boolean isCancelled();

    /**
     * Cancel the current search.
     */
    void cancel();

    /**
     * @return the end time to which this extractor will search
     */
    long getEndTime();

    /**
     * Check whether the search skipped CCS clusters.
     * @throws ResourceNotFoundException if any CCS clusters were skipped, as this could
     *                                   cause anomalies to be spuriously detected.
     * @param searchResponse The search response to check for skipped CCS clusters.
     */
    default void checkForSkippedClusters(SearchResponse searchResponse) {
        SearchResponse.Clusters clusterResponse = searchResponse.getClusters();
        if (clusterResponse != null && clusterResponse.getSkipped() > 0) {
            throw new ResourceNotFoundException(
                "[{}] remote clusters out of [{}] were skipped when performing datafeed search",
                clusterResponse.getSkipped(),
                clusterResponse.getTotal()
            );
        }
    }

}
