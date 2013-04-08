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
 * <p>By default, the {@link #docs(boolean)}, {@link #store(boolean)}, {@link #indexing(boolean)}
 * are enabled. Other stats can be enabled as well.
 * <p/>
 * <p>All the stats to be returned can be cleared using {@link #clear()}, at which point, specific
 * stats can be enabled.
 */
public class IndicesStatsRequest extends BroadcastOperationRequest<IndicesStatsRequest> {

    private CommonStatsFlags flags = new CommonStatsFlags();

    /**
     * Sets all flags to return all stats.
     */
    public IndicesStatsRequest all() {
        flags.all();
        return this;
    }

    /**
     * Clears all stats.
     */
    public IndicesStatsRequest clear() {
        flags.clear();
        return this;
    }

    /**
     * Document types to return stats for. Mainly affects {@link #indexing(boolean)} when
     * enabled, returning specific indexing stats for those types.
     */
    public IndicesStatsRequest types(String... types) {
        flags.types(types);
        return this;
    }

    /**
     * Document types to return stats for. Mainly affects {@link #indexing(boolean)} when
     * enabled, returning specific indexing stats for those types.
     */
    public String[] types() {
        return this.flags.types();
    }

    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
     * when enabled.
     */
    public IndicesStatsRequest groups(String... groups) {
        flags.groups(groups);
        return this;
    }

    public String[] groups() {
        return this.flags.groups();
    }

    public IndicesStatsRequest docs(boolean docs) {
        flags.docs(docs);
        return this;
    }

    public boolean docs() {
        return flags.docs();
    }

    public IndicesStatsRequest store(boolean store) {
        flags.store(store);
        return this;
    }

    public boolean store() {
        return flags.store();
    }

    public IndicesStatsRequest indexing(boolean indexing) {
        flags.indexing(indexing);
        return this;
    }

    public boolean indexing() {
        return flags.indexing();
    }

    public IndicesStatsRequest get(boolean get) {
        flags.get(get);
        return this;
    }

    public boolean get() {
        return flags.get();
    }

    public IndicesStatsRequest search(boolean search) {
        flags.search(search);
        return this;
    }

    public boolean search() {
        return flags.search();
    }

    public IndicesStatsRequest merge(boolean merge) {
        flags.merge(merge);
        return this;
    }

    public boolean merge() {
        return flags.merge();
    }

    public IndicesStatsRequest refresh(boolean refresh) {
        flags.refresh(refresh);
        return this;
    }

    public boolean refresh() {
        return flags.refresh();
    }

    public IndicesStatsRequest flush(boolean flush) {
        flags.flush(flush);
        return this;
    }

    public boolean flush() {
        return flags.flush();
    }

    public IndicesStatsRequest warmer(boolean warmer) {
        flags.warmer(warmer);
        return this;
    }

    public boolean warmer() {
        return flags.warmer();
    }

    public IndicesStatsRequest filterCache(boolean filterCache) {
        flags.filterCache(filterCache);
        return this;
    }

    public boolean filterCache() {
        return flags.filterCache();
    }

    public IndicesStatsRequest idCache(boolean idCache) {
        flags.idCache(idCache);
        return this;
    }

    public boolean idCache() {
        return flags.idCache();
    }

    public IndicesStatsRequest fieldData(boolean fieldData) {
        flags.fieldData(fieldData);
        return this;
    }

    public boolean fieldData() {
        return flags.fieldData();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        flags.writeTo(out);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        flags = CommonStatsFlags.readCommonStatsFlags(in);
    }
}
