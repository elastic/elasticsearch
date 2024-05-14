/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;


import java.util.Map;

/**
 * An interface to allow accumulating results of document parsing (collected with {@link DocumentSizeObserver})
 */
public interface DocumentSizeAccumulator {
    DocumentSizeAccumulator EMPTY_INSTANCE = new DocumentSizeAccumulator() {

        @Override
        public void add(long size) {}

        @Override
        public Map<String, String> totalDocSizeMap(Map<String, String> map) {
            return map;
        }
    };

    /**
     * Accumulates the reported size of the document
     * @param size the size of the doc
     */
    void add(long size);

    /**
     * Adds an entry to a map with a value being the current state of the accumulator.
     * Then resets the accumulator.
     * @param map a map with previous value of size
     * @return an map with a new value of size
     */
    Map<String, String> totalDocSizeMap(Map<String, String> map);
}
