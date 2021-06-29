/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.datafeed.extractor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public interface DataExtractor {

    /**
     * @return {@code true} if the search has not finished yet, or {@code false} otherwise
     */
    boolean hasNext();

    /**
     * Returns the next available extracted data. Note that it is possible for the
     * extracted data to be empty the last time this method can be called.
     * @return an optional input stream with the next available extracted data
     * @throws IOException if an error occurs while extracting the data
     */
    Optional<InputStream> next() throws IOException;

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
}
