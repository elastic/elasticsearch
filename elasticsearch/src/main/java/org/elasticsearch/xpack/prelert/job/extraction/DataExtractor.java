/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.extraction;

import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

public interface DataExtractor
{
    /**
     * Set-up the extractor for a new search
     *
     * @param start start time
     * @param end end time
     * @param logger logger
     */
    void newSearch(long start, long end, Logger logger) throws IOException;

    /**
     * Cleans up after a search.
     */
    void clear();

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
     * Cancel the current search.
     */
    void cancel();
}
