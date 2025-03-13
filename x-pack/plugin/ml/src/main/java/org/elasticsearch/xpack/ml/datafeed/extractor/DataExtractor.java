/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor;

import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public interface DataExtractor {

    record Result(SearchInterval searchInterval, Optional<InputStream> data) {}

    record DataSummary(Long earliestTime, Long latestTime, long totalHits) {
        public boolean hasData() {
            return earliestTime != null;
        }
    }

    DataSummary getSummary();

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
     * Cancels and immediately destroys the data extractor, releasing all its resources.
     */
    void destroy();

    /**
     * @return the end time to which this extractor will search
     */
    long getEndTime();
}
