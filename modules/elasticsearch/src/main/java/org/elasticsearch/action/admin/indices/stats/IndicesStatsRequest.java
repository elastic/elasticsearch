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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.support.broadcast.BroadcastOperationRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get indices level stats. Allow to enable different stats to be returned.
 *
 * <p>By default, the {@link #docs(boolean)}, {@link #store(boolean)}, {@link #indexing(boolean)}
 * are enabled. Other stats can be enabled as well.
 *
 * <p>All the stats to be returned can be cleared using {@link #clear()}, at which point, specific
 * stats can be enabled.
 */
public class IndicesStatsRequest extends BroadcastOperationRequest {

    private boolean docs = true;
    private boolean store = true;
    private boolean indexing = true;
    private boolean merge = false;
    private boolean refresh = false;
    private boolean flush = false;
    private String[] types = null;

    public IndicesStatsRequest indices(String... indices) {
        this.indices = indices;
        return this;
    }

    /**
     * Clears all stats.
     */
    public IndicesStatsRequest clear() {
        docs = false;
        store = false;
        indexing = false;
        merge = false;
        refresh = false;
        flush = false;
        return this;
    }

    /**
     * Document types to return stats for. Mainly affects {@link #indexing(boolean)} when
     * enabled, returning specific indexing stats for those types.
     */
    public IndicesStatsRequest types(String... types) {
        this.types = types;
        return this;
    }

    /**
     * Document types to return stats for. Mainly affects {@link #indexing(boolean)} when
     * enabled, returning specific indexing stats for those types.
     */
    public String[] types() {
        return this.types;
    }

    public IndicesStatsRequest docs(boolean docs) {
        this.docs = docs;
        return this;
    }

    public boolean docs() {
        return this.docs;
    }

    public IndicesStatsRequest store(boolean store) {
        this.store = store;
        return this;
    }

    public boolean store() {
        return this.store;
    }

    public IndicesStatsRequest indexing(boolean indexing) {
        this.indexing = indexing;
        return this;
    }

    public boolean indexing() {
        return this.indexing;
    }

    public IndicesStatsRequest merge(boolean merge) {
        this.merge = merge;
        return this;
    }

    public boolean merge() {
        return this.merge;
    }

    public IndicesStatsRequest refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public IndicesStatsRequest flush(boolean flush) {
        this.flush = flush;
        return this;
    }

    public boolean flush() {
        return this.flush;
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(docs);
        out.writeBoolean(store);
        out.writeBoolean(indexing);
        out.writeBoolean(merge);
        out.writeBoolean(flush);
        out.writeBoolean(refresh);
        if (types == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(types.length);
            for (String type : types) {
                out.writeUTF(type);
            }
        }
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        docs = in.readBoolean();
        store = in.readBoolean();
        indexing = in.readBoolean();
        merge = in.readBoolean();
        flush = in.readBoolean();
        refresh = in.readBoolean();
        int size = in.readVInt();
        if (size > 0) {
            types = new String[size];
            for (int i = 0; i < size; i++) {
                types[i] = in.readUTF();
            }
        }
    }
}
