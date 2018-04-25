/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Interface implemented by requests that modify the documents in an index like {@link IndexRequest}, {@link UpdateRequest}, and
 * {@link BulkRequest}. Rather than implement this directly most implementers should extend {@link ReplicatedWriteRequest}.
 */
public interface WriteRequest<R extends WriteRequest<R>> extends Streamable {
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

        public static RefreshPolicy readFrom(StreamInput in) throws IOException {
            return RefreshPolicy.values()[in.readByte()];
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte((byte) ordinal());
        }
    }
}
