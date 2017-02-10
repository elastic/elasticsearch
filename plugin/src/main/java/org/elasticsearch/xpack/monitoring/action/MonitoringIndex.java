/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * {@code MonitoringIndex} represents the receivable index from any request.
 * <p>
 * This allows external systems to provide details for an index without having to know its exact name.
 */
public enum MonitoringIndex implements Writeable {

    /**
     * Data that drives information about the "cluster" (e.g., a node or instance).
     */
    DATA {
        @Override
        public boolean matchesIndexName(String indexName) {
            return "_data".equals(indexName);
        }
    },

    /**
     * Timestamped data that drives the charts (e.g., memory statistics).
     */
    TIMESTAMPED {
        @Override
        public boolean matchesIndexName(String indexName) {
            return Strings.isEmpty(indexName);
        }
    };

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte((byte)ordinal());
    }

    public static MonitoringIndex readFrom(StreamInput in) throws IOException {
        return values()[in.readByte()];
    }

    /**
     * Determine if the {@code indexName} matches {@code this} monitoring index.
     *
     * @param indexName The name of the index.
     * @return {@code true} if {@code this} matches the {@code indexName}
     */
    public abstract boolean matchesIndexName(String indexName);

    /**
     * Find the {@link MonitoringIndex} to use for the request.
     *
     * @param indexName The name of the index.
     * @return Never {@code null}.
     * @throws IllegalArgumentException if {@code indexName} is unrecognized
     */
    public static MonitoringIndex from(String indexName) {
        for (MonitoringIndex index : values()) {
            if (index.matchesIndexName(indexName)) {
                return index;
            }
        }

        throw new IllegalArgumentException("unrecognized index name [" + indexName + "]");
    }

}
