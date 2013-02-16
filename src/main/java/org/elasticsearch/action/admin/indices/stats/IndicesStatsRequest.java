/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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
 * <p/>
 * <p>By default, the {@link #setDocs(boolean)}, {@link #setStore(boolean)}, {@link #setIndexing(boolean)}
 * are enabled. Other stats can be enabled as well.
 * <p/>
 * <p>All the stats to be returned can be cleared using {@link #clear()}, at which point, specific
 * stats can be enabled.
 */
public class IndicesStatsRequest extends BroadcastOperationRequest<IndicesStatsRequest> {

    private boolean docs = true;
    private boolean store = true;
    private boolean indexing = true;
    private boolean get = true;
    private boolean search = true;
    private boolean merge = false;
    private boolean refresh = false;
    private boolean flush = false;
    private boolean warmer = false;
    private String[] types = null;
    private String[] groups = null;

    /**
     * Sets all flags to return all stats.
     */
    public IndicesStatsRequest all() {
        docs = true;
        store = true;
        get = true;
        indexing = true;
        search = true;
        merge = true;
        refresh = true;
        flush = true;
        warmer = true;
        types = null;
        groups = null;
        return this;
    }

    /**
     * Clears all stats.
     */
    public IndicesStatsRequest clear() {
        docs = false;
        store = false;
        get = false;
        indexing = false;
        search = false;
        merge = false;
        refresh = false;
        flush = false;
        warmer = false;
        types = null;
        groups = null;
        return this;
    }

    /**
     * Document types to return stats for. Mainly affects {@link #setIndexing(boolean)} when
     * enabled, returning specific indexing stats for those types.
     */
    public IndicesStatsRequest setTypes(String... types) {
        this.types = types;
        return this;
    }

    /**
     * Document types to return stats for. Mainly affects {@link #setIndexing(boolean)} when
     * enabled, returning specific indexing stats for those types.
     */
    public String[] getTypes() {
        return this.types;
    }

    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
     * when enabled.
     */
    public IndicesStatsRequest setGroups(String... groups) {
        this.groups = groups;
        return this;
    }

    public String[] getGroups() {
        return this.groups;
    }

    public IndicesStatsRequest setDocs(boolean docs) {
        this.docs = docs;
        return this;
    }

    public boolean isDocs() {
        return this.docs;
    }

    public IndicesStatsRequest setStore(boolean store) {
        this.store = store;
        return this;
    }

    public boolean isStore() {
        return this.store;
    }

    public IndicesStatsRequest setIndexing(boolean indexing) {
        this.indexing = indexing;
        return this;
    }

    public boolean isIndexing() {
        return this.indexing;
    }

    public IndicesStatsRequest setGet(boolean get) {
        this.get = get;
        return this;
    }

    public boolean isGet() {
        return this.get;
    }

    public IndicesStatsRequest setSearch(boolean search) {
        this.search = search;
        return this;
    }

    public boolean isSearch() {
        return this.search;
    }

    public IndicesStatsRequest setMerge(boolean merge) {
        this.merge = merge;
        return this;
    }

    public boolean isMerge() {
        return this.merge;
    }

    public IndicesStatsRequest setRefresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean isRefresh() {
        return this.refresh;
    }

    public IndicesStatsRequest setFlush(boolean flush) {
        this.flush = flush;
        return this;
    }

    public boolean isFlush() {
        return this.flush;
    }

    public IndicesStatsRequest setWarmer(boolean warmer) {
        this.warmer = warmer;
        return this;
    }

    public boolean isWarmer() {
        return this.warmer;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(docs);
        out.writeBoolean(store);
        out.writeBoolean(indexing);
        out.writeBoolean(get);
        out.writeBoolean(search);
        out.writeBoolean(merge);
        out.writeBoolean(flush);
        out.writeBoolean(refresh);
        out.writeBoolean(warmer);
        if (types == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(types.length);
            for (String type : types) {
                out.writeString(type);
            }
        }
        if (groups == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(groups.length);
            for (String group : groups) {
                out.writeString(group);
            }
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        docs = in.readBoolean();
        store = in.readBoolean();
        indexing = in.readBoolean();
        get = in.readBoolean();
        search = in.readBoolean();
        merge = in.readBoolean();
        flush = in.readBoolean();
        refresh = in.readBoolean();
        warmer = in.readBoolean();
        int size = in.readVInt();
        if (size > 0) {
            types = new String[size];
            for (int i = 0; i < size; i++) {
                types[i] = in.readString();
            }
        }
        size = in.readVInt();
        if (size > 0) {
            groups = new String[size];
            for (int i = 0; i < size; i++) {
                groups[i] = in.readString();
            }
        }
    }
}
