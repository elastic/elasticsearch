/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.Strings;

/**
 * Operations on data streams
 */
public abstract class DataStreamAction {
    private final String dataStream;

    public static DataStreamAction addBackingIndex(String dataStream, String index) {
        return new DataStreamAction.AddBackingIndex(dataStream, index);
    }

    public static DataStreamAction removeBackingIndex(String dataStream, String index) {
        return new DataStreamAction.RemoveBackingIndex(dataStream, index);
    }

    private DataStreamAction(String dataStream) {
        if (false == Strings.hasText(dataStream)) {
            throw new IllegalArgumentException("[data_stream] is required");
        }
        this.dataStream = dataStream;
    }

    /**
     * Data stream on which the operation should act
     */
    public String getDataStream() {
        return dataStream;
    }

    public static class AddBackingIndex extends DataStreamAction {

        private final String index;

        private AddBackingIndex(String dataStream, String index) {
            super(dataStream);

            if (false == Strings.hasText(index)) {
                throw new IllegalArgumentException("[index] is required");
            }

            this.index = index;
        }

        public String getIndex() {
            return index;
        }

    }

    public static class RemoveBackingIndex extends DataStreamAction {

        private final String index;

        private RemoveBackingIndex(String dataStream, String index) {
            super(dataStream);

            if (false == Strings.hasText(index)) {
                throw new IllegalArgumentException("[index] is required");
            }

            this.index = index;
        }

        public String getIndex() {
            return index;
        }

    }

}
