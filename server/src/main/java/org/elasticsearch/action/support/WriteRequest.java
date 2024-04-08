/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Interface implemented by requests that modify the documents in an index like {@link IndexRequest}, {@link UpdateRequest}, and
 * {@link BulkRequest}. Rather than implement this directly most implementers should extend {@link ReplicatedWriteRequest}.
 */
public interface WriteRequest<R extends WriteRequest<R>> extends Writeable {
    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}, the default).
     */
    R setRefreshPolicy(RefreshPolicy refreshPolicy);

    /**
     * Parse the refresh policy from a string, only modifying it if the string is non null. Convenient to use with request parsing.
     */
    @SuppressWarnings("unchecked")
    default R setRefreshPolicy(String refreshPolicy) {
        if (refreshPolicy != null) {
            setRefreshPolicy(RefreshPolicy.parse(refreshPolicy));
        }
        return (R) this;
    }

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}, the default).
     */
    RefreshPolicy getRefreshPolicy();

    ActionRequestValidationException validate();

    enum RefreshPolicy implements Writeable {
        /**
         * Don't refresh after this request. The default.
         */
        NONE("false"),
        /**
         * Force a refresh as part of this request. This refresh policy does not scale for high indexing or search throughput but is useful
         * to present a consistent view to for indices with very low traffic. And it is wonderful for tests!
         */
        IMMEDIATE("true"),
        /**
         * Leave this request open until a refresh has made the contents of this request visible to search. This refresh policy is
         * compatible with high indexing and search throughput but it causes the request to wait to reply until a refresh occurs.
         */
        WAIT_UNTIL("wait_for");

        private final String value;

        RefreshPolicy(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        /**
         * Parse the string representation of a refresh policy, usually from a request parameter.
         */
        public static RefreshPolicy parse(String value) {
            for (RefreshPolicy policy : values()) {
                if (policy.getValue().equals(value)) {
                    return policy;
                }
            }
            if ("".equals(value)) {
                // Empty string is IMMEDIATE because that makes "POST /test/test/1?refresh" perform
                // a refresh which reads well and is what folks are used to.
                return IMMEDIATE;
            }
            throw new IllegalArgumentException("Unknown value for refresh: [" + value + "].");
        }

        private static final RefreshPolicy[] values = values();

        public static RefreshPolicy readFrom(StreamInput in) throws IOException {
            return values[in.readByte()];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte((byte) ordinal());
        }
    }
}
