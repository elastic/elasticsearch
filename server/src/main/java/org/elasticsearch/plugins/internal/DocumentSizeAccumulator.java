/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.apache.lucene.index.SegmentInfos;

import java.util.Map;

/**
 * An interface to allow accumulating results of document parsing (collected with {@link XContentParserDecorator})
 */
public interface DocumentSizeAccumulator {
    DocumentSizeAccumulator EMPTY_INSTANCE = new DocumentSizeAccumulator() {

        @Override
        public void add(long size) {}

        @Override
        public Map<String, String> getAsCommitUserData(SegmentInfos segmentInfos) {
            return Map.of();
        }
    };

    /**
     * Accumulates the reported size of the document
     * @param size the size of the doc
     */
    void add(long size);

    /**
     * Returns a map with an entry being the current state of the accumulator + previously commited value for that key
     * Then resets the accumulator.
     *
     * @param segmentInfos a shard's previously comited SegmentInfos
     * @return an map with a new value of size
     */
    Map<String, String> getAsCommitUserData(SegmentInfos segmentInfos);
}
