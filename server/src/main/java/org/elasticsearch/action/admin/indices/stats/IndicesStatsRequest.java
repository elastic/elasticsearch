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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags.Flag;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A request to get indices level stats. Allow to enable different stats to be returned.
 * <p>
 * By default, all statistics are enabled.
 * <p>
 * All the stats to be returned can be cleared using {@link #clear()}, at which point, specific
 * stats can be enabled.
 */
public class IndicesStatsRequest extends BroadcastRequest<IndicesStatsRequest> {

    private CommonStatsFlags flags = new CommonStatsFlags();

    public IndicesStatsRequest() {
        super((String[])null);
    }

    public IndicesStatsRequest(StreamInput in) throws IOException {
        super(in);
        flags = new CommonStatsFlags(in);
    }

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
     * Returns the underlying stats flags.
     */
    public CommonStatsFlags flags() {
        return flags;
    }

    /**
     * Sets the underlying stats flags.
     */
    public IndicesStatsRequest flags(CommonStatsFlags flags) {
        this.flags = flags;
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
        flags.set(Flag.Docs, docs);
        return this;
    }

    public boolean docs() {
        return flags.isSet(Flag.Docs);
    }

    public IndicesStatsRequest store(boolean store) {
        flags.set(Flag.Store, store);
        return this;
    }

    public boolean store() {
        return flags.isSet(Flag.Store);
    }

    public IndicesStatsRequest indexing(boolean indexing) {
        flags.set(Flag.Indexing, indexing);

        return this;
    }

    public boolean indexing() {
        return flags.isSet(Flag.Indexing);
    }

    public IndicesStatsRequest get(boolean get) {
        flags.set(Flag.Get, get);
        return this;
    }

    public boolean get() {
        return flags.isSet(Flag.Get);
    }

    public IndicesStatsRequest search(boolean search) {
        flags.set(Flag.Search, search);
        return this;
    }

    public boolean search() {
        return flags.isSet(Flag.Search);
    }

    public IndicesStatsRequest merge(boolean merge) {
        flags.set(Flag.Merge, merge);
        return this;
    }

    public boolean merge() {
        return flags.isSet(Flag.Merge);
    }

    public IndicesStatsRequest refresh(boolean refresh) {
        flags.set(Flag.Refresh, refresh);
        return this;
    }

    public boolean refresh() {
        return flags.isSet(Flag.Refresh);
    }

    public IndicesStatsRequest flush(boolean flush) {
        flags.set(Flag.Flush, flush);
        return this;
    }

    public boolean flush() {
        return flags.isSet(Flag.Flush);
    }

    public IndicesStatsRequest warmer(boolean warmer) {
        flags.set(Flag.Warmer, warmer);
        return this;
    }

    public boolean warmer() {
        return flags.isSet(Flag.Warmer);
    }

    public IndicesStatsRequest queryCache(boolean queryCache) {
        flags.set(Flag.QueryCache, queryCache);
        return this;
    }

    public boolean queryCache() {
        return flags.isSet(Flag.QueryCache);
    }

    public IndicesStatsRequest fieldData(boolean fieldData) {
        flags.set(Flag.FieldData, fieldData);
        return this;
    }

    public boolean fieldData() {
        return flags.isSet(Flag.FieldData);
    }

    public IndicesStatsRequest segments(boolean segments) {
        flags.set(Flag.Segments, segments);
        return this;
    }

    public boolean segments() {
        return flags.isSet(Flag.Segments);
    }

    public IndicesStatsRequest fieldDataFields(String... fieldDataFields) {
        flags.fieldDataFields(fieldDataFields);
        return this;
    }

    public String[] fieldDataFields() {
        return flags.fieldDataFields();
    }

    public IndicesStatsRequest completion(boolean completion) {
        flags.set(Flag.Completion, completion);
        return this;
    }

    public boolean completion() {
        return flags.isSet(Flag.Completion);
    }

    public IndicesStatsRequest completionFields(String... completionDataFields) {
        flags.completionDataFields(completionDataFields);
        return this;
    }

    public String[] completionFields() {
        return flags.completionDataFields();
    }

    public IndicesStatsRequest translog(boolean translog) {
        flags.set(Flag.Translog, translog);
        return this;
    }

    public boolean translog() {
        return flags.isSet(Flag.Translog);
    }

    public IndicesStatsRequest requestCache(boolean requestCache) {
        flags.set(Flag.RequestCache, requestCache);
        return this;
    }

    public boolean requestCache() {
        return flags.isSet(Flag.RequestCache);
    }

    public IndicesStatsRequest recovery(boolean recovery) {
        flags.set(Flag.Recovery, recovery);
        return this;
    }

    public boolean recovery() {
        return flags.isSet(Flag.Recovery);
    }

    public boolean includeSegmentFileSizes() {
        return flags.includeSegmentFileSizes();
    }

    public IndicesStatsRequest includeSegmentFileSizes(boolean includeSegmentFileSizes) {
        flags.includeSegmentFileSizes(includeSegmentFileSizes);
        return this;
    }

    public IndicesStatsRequest includeUnloadedSegments(boolean includeUnloadedSegments) {
        flags.includeUnloadedSegments(includeUnloadedSegments);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        flags.writeTo(out);
    }
}
