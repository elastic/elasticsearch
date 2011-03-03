/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.indices;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.index.cache.CacheStats;
import org.elasticsearch.index.merge.MergeStats;

import java.io.IOException;
import java.io.Serializable;

/**
 * Global information on indices stats running on a specific node.
 *
 * @author kimchy (shay.banon)
 */
public class NodeIndicesStats implements Streamable, Serializable, ToXContent {

    private ByteSizeValue storeSize;

    private long numDocs;

    private CacheStats cacheStats;

    private MergeStats mergeStats;

    NodeIndicesStats() {
    }

    public NodeIndicesStats(ByteSizeValue storeSize, long numDocs, CacheStats cacheStats, MergeStats mergeStats) {
        this.storeSize = storeSize;
        this.numDocs = numDocs;
        this.cacheStats = cacheStats;
        this.mergeStats = mergeStats;
    }

    /**
     * The size of the index storage taken on the node.
     */
    public ByteSizeValue storeSize() {
        return this.storeSize;
    }

    /**
     * The size of the index storage taken on the node.
     */
    public ByteSizeValue getStoreSize() {
        return storeSize;
    }

    /**
     * The number of docs on the node (an aggregation of the number of docs of all the shards allocated on the node).
     */
    public long numDocs() {
        return numDocs;
    }

    /**
     * The number of docs on the node (an aggregation of the number of docs of all the shards allocated on the node).
     */
    public long getNumDocs() {
        return numDocs();
    }

    public CacheStats cache() {
        return this.cacheStats;
    }

    public CacheStats getCache() {
        return this.cache();
    }

    public MergeStats merge() {
        return this.mergeStats;
    }

    public MergeStats getMerge() {
        return this.mergeStats;
    }

    public static NodeIndicesStats readIndicesStats(StreamInput in) throws IOException {
        NodeIndicesStats stats = new NodeIndicesStats();
        stats.readFrom(in);
        return stats;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        storeSize = ByteSizeValue.readBytesSizeValue(in);
        numDocs = in.readVLong();
        cacheStats = CacheStats.readCacheStats(in);
        mergeStats = MergeStats.readMergeStats(in);
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        storeSize.writeTo(out);
        out.writeVLong(numDocs);
        cacheStats.writeTo(out);
        mergeStats.writeTo(out);
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.INDICES);

        builder.field(Fields.SIZE, storeSize.toString());
        builder.field(Fields.SIZE_IN_BYTES, storeSize.bytes());

        builder.startObject(Fields.DOCS);
        builder.field(Fields.NUM_DOCS, numDocs);
        builder.endObject();

        cacheStats.toXContent(builder, params);
        mergeStats.toXContent(builder, params);

        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");

        static final XContentBuilderString SIZE = new XContentBuilderString("size");
        static final XContentBuilderString SIZE_IN_BYTES = new XContentBuilderString("size_in_bytes");

        static final XContentBuilderString DOCS = new XContentBuilderString("docs");
        static final XContentBuilderString NUM_DOCS = new XContentBuilderString("num_docs");
    }
}
