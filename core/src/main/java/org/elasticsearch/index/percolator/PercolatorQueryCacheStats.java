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
package org.elasticsearch.index.percolator;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Exposes percolator query cache statistics.
 */
public class PercolatorQueryCacheStats implements Streamable, ToXContent {

    private long numQueries;

    /**
     * Noop constructor for serialization purposes.
     */
    public PercolatorQueryCacheStats() {
    }

    PercolatorQueryCacheStats(long numQueries) {
        this.numQueries = numQueries;
    }

    /**
     * @return The total number of loaded percolate queries.
     */
    public long getNumQueries() {
        return numQueries;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PERCOLATOR);
        builder.field(Fields.QUERIES, getNumQueries());
        builder.endObject();
        return builder;
    }

    public void add(PercolatorQueryCacheStats percolate) {
        if (percolate == null) {
            return;
        }

        numQueries += percolate.getNumQueries();
    }

    static final class Fields {
        static final String PERCOLATOR = "percolator";
        static final String QUERIES = "num_queries";
    }

    public static PercolatorQueryCacheStats readPercolateStats(StreamInput in) throws IOException {
        PercolatorQueryCacheStats stats = new PercolatorQueryCacheStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        numQueries = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(numQueries);
    }
}
