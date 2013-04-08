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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 */
public class CommonStatsFlags implements Streamable {

    private boolean docs = true;
    private boolean store = true;
    private boolean indexing = true;
    private boolean get = true;
    private boolean search = true;
    private boolean merge = false;
    private boolean refresh = false;
    private boolean flush = false;
    private boolean warmer = false;
    private boolean filterCache = false;
    private boolean idCache = false;
    private boolean fieldData = false;
    private String[] types = null;
    private String[] groups = null;

    /**
     * Sets all flags to return all stats.
     */
    public CommonStatsFlags all() {
        docs = true;
        store = true;
        get = true;
        indexing = true;
        search = true;
        merge = true;
        refresh = true;
        flush = true;
        warmer = true;
        filterCache = true;
        idCache = true;
        fieldData = true;
        types = null;
        groups = null;
        return this;
    }

    /**
     * Clears all stats.
     */
    public CommonStatsFlags clear() {
        docs = false;
        store = false;
        get = false;
        indexing = false;
        search = false;
        merge = false;
        refresh = false;
        flush = false;
        warmer = false;
        filterCache = false;
        idCache = false;
        fieldData = false;
        types = null;
        groups = null;
        return this;
    }

    public boolean anySet() {
        return docs || store || get || indexing || search || merge || refresh || flush || warmer || filterCache || idCache || fieldData;
    }

    /**
     * Document types to return stats for. Mainly affects {@link #indexing(boolean)} when
     * enabled, returning specific indexing stats for those types.
     */
    public CommonStatsFlags types(String... types) {
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

    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
     * when enabled.
     */
    public CommonStatsFlags groups(String... groups) {
        this.groups = groups;
        return this;
    }

    public String[] groups() {
        return this.groups;
    }

    public CommonStatsFlags docs(boolean docs) {
        this.docs = docs;
        return this;
    }

    public boolean docs() {
        return this.docs;
    }

    public CommonStatsFlags store(boolean store) {
        this.store = store;
        return this;
    }

    public boolean store() {
        return this.store;
    }

    public CommonStatsFlags indexing(boolean indexing) {
        this.indexing = indexing;
        return this;
    }

    public boolean indexing() {
        return this.indexing;
    }

    public CommonStatsFlags get(boolean get) {
        this.get = get;
        return this;
    }

    public boolean get() {
        return this.get;
    }

    public CommonStatsFlags search(boolean search) {
        this.search = search;
        return this;
    }

    public boolean search() {
        return this.search;
    }

    public CommonStatsFlags merge(boolean merge) {
        this.merge = merge;
        return this;
    }

    public boolean merge() {
        return this.merge;
    }

    public CommonStatsFlags refresh(boolean refresh) {
        this.refresh = refresh;
        return this;
    }

    public boolean refresh() {
        return this.refresh;
    }

    public CommonStatsFlags flush(boolean flush) {
        this.flush = flush;
        return this;
    }

    public boolean flush() {
        return this.flush;
    }

    public CommonStatsFlags warmer(boolean warmer) {
        this.warmer = warmer;
        return this;
    }

    public boolean warmer() {
        return this.warmer;
    }

    public CommonStatsFlags filterCache(boolean filterCache) {
        this.filterCache = filterCache;
        return this;
    }

    public boolean filterCache() {
        return this.filterCache;
    }

    public CommonStatsFlags idCache(boolean idCache) {
        this.idCache = idCache;
        return this;
    }

    public boolean idCache() {
        return this.idCache;
    }

    public CommonStatsFlags fieldData(boolean fieldData) {
        this.fieldData = fieldData;
        return this;
    }

    public boolean fieldData() {
        return this.fieldData;
    }

    public static CommonStatsFlags readCommonStatsFlags(StreamInput in) throws IOException {
        CommonStatsFlags flags = new CommonStatsFlags();
        flags.readFrom(in);
        return flags;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(docs);
        out.writeBoolean(store);
        out.writeBoolean(indexing);
        out.writeBoolean(get);
        out.writeBoolean(search);
        out.writeBoolean(merge);
        out.writeBoolean(flush);
        out.writeBoolean(refresh);
        out.writeBoolean(warmer);
        out.writeBoolean(filterCache);
        out.writeBoolean(idCache);
        out.writeBoolean(fieldData);
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
        docs = in.readBoolean();
        store = in.readBoolean();
        indexing = in.readBoolean();
        get = in.readBoolean();
        search = in.readBoolean();
        merge = in.readBoolean();
        flush = in.readBoolean();
        refresh = in.readBoolean();
        warmer = in.readBoolean();
        filterCache = in.readBoolean();
        idCache = in.readBoolean();
        fieldData = in.readBoolean();
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
