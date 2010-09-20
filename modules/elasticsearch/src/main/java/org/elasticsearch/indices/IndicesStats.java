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

import java.io.IOException;
import java.io.Serializable;

/**
 * Global information on indices stats running on a specific node.
 *
 * @author kimchy (shay.banon)
 */
public class IndicesStats implements Streamable, Serializable, ToXContent {

    private ByteSizeValue storeSize;

    IndicesStats() {
    }

    public IndicesStats(ByteSizeValue storeSize) {
        this.storeSize = storeSize;
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

    public static IndicesStats readIndicesStats(StreamInput in) throws IOException {
        IndicesStats stats = new IndicesStats();
        stats.readFrom(in);
        return stats;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        storeSize = ByteSizeValue.readBytesSizeValue(in);
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        storeSize.writeTo(out);
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("indices");
        builder.field("store_size", storeSize.toString());
        builder.field("store_size_in_bytes", storeSize.bytes());
        builder.endObject();
    }
}
