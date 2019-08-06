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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;

public class CommonStatsFlags implements Writeable, Cloneable {

    public static final CommonStatsFlags ALL = new CommonStatsFlags().all();
    public static final CommonStatsFlags NONE = new CommonStatsFlags().clear();

    private EnumSet<Flag> flags = EnumSet.allOf(Flag.class);
    private String[] types = null;
    private String[] groups = null;
    private String[] fieldDataFields = null;
    private String[] completionDataFields = null;
    private boolean includeSegmentFileSizes = false;
    private boolean includeUnloadedSegments = false;

    /**
     * @param flags flags to set. If no flags are supplied, default flags will be set.
     */
    public CommonStatsFlags(Flag... flags) {
        if (flags.length > 0) {
            clear();
            Collections.addAll(this.flags, flags);
        }
    }

    public CommonStatsFlags(StreamInput in) throws IOException {
        final long longFlags = in.readLong();
        flags.clear();
        for (Flag flag : Flag.values()) {
            if ((longFlags & (1 << flag.getIndex())) != 0) {
                flags.add(flag);
            }
        }
        types = in.readStringArray();
        groups = in.readStringArray();
        fieldDataFields = in.readStringArray();
        completionDataFields = in.readStringArray();
        includeSegmentFileSizes = in.readBoolean();
        if (in.getVersion().onOrAfter(Version.V_7_2_0)) {
            includeUnloadedSegments = in.readBoolean();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        long longFlags = 0;
        for (Flag flag : flags) {
            longFlags |= (1 << flag.getIndex());
        }
        out.writeLong(longFlags);

        out.writeStringArrayNullable(types);
        out.writeStringArrayNullable(groups);
        out.writeStringArrayNullable(fieldDataFields);
        out.writeStringArrayNullable(completionDataFields);
        out.writeBoolean(includeSegmentFileSizes);
        if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
            out.writeBoolean(includeUnloadedSegments);
        }
    }

    /**
     * Sets all flags to return all stats.
     */
    public CommonStatsFlags all() {
        flags = EnumSet.allOf(Flag.class);
        types = null;
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        includeUnloadedSegments = false;
        return this;
    }

    /**
     * Clears all stats.
     */
    public CommonStatsFlags clear() {
        flags = EnumSet.noneOf(Flag.class);
        types = null;
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        includeUnloadedSegments = false;
        return this;
    }

    public boolean anySet() {
        return !flags.isEmpty();
    }

    public Flag[] getFlags() {
        return flags.toArray(new Flag[flags.size()]);
    }

    /**
     * Document types to return stats for. Mainly affects {@link Flag#Indexing} when
     * enabled, returning specific indexing stats for those types.
     */
    public CommonStatsFlags types(String... types) {
        this.types = types;
        return this;
    }

    /**
     * Document types to return stats for. Mainly affects {@link Flag#Indexing} when
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

    /**
     * Sets specific search group stats to retrieve the stats for. Mainly affects search
     * when enabled.
     */
    public CommonStatsFlags fieldDataFields(String... fieldDataFields) {
        this.fieldDataFields = fieldDataFields;
        return this;
    }

    public String[] fieldDataFields() {
        return this.fieldDataFields;
    }

    public CommonStatsFlags completionDataFields(String... completionDataFields) {
        this.completionDataFields = completionDataFields;
        return this;
    }

    public String[] completionDataFields() {
        return this.completionDataFields;
    }

    public CommonStatsFlags includeSegmentFileSizes(boolean includeSegmentFileSizes) {
        this.includeSegmentFileSizes = includeSegmentFileSizes;
        return this;
    }

    public CommonStatsFlags includeUnloadedSegments(boolean includeUnloadedSegments) {
        this.includeUnloadedSegments = includeUnloadedSegments;
        return this;
    }

    public boolean includeUnloadedSegments() {
        return this.includeUnloadedSegments;
    }

    public boolean includeSegmentFileSizes() {
        return this.includeSegmentFileSizes;
    }

    public boolean isSet(Flag flag) {
        return flags.contains(flag);
    }

    boolean unSet(Flag flag) {
        return flags.remove(flag);
    }

    void set(Flag flag) {
        flags.add(flag);
    }

    public CommonStatsFlags set(Flag flag, boolean add) {
        if (add) {
            set(flag);
        } else {
            unSet(flag);
        }
        return this;
    }

    @Override
    public CommonStatsFlags clone() {
        try {
            CommonStatsFlags cloned = (CommonStatsFlags) super.clone();
            cloned.flags = flags.clone();
            return cloned;
        } catch (CloneNotSupportedException e) {
            throw new AssertionError(e);
        }
    }

    public enum Flag {
        Store("store", 0),
        Indexing("indexing", 1),
        Get("get", 2),
        Search("search", 3),
        Merge("merge", 4),
        Flush("flush", 5),
        Refresh("refresh", 6),
        QueryCache("query_cache", 7),
        FieldData("fielddata", 8),
        Docs("docs", 9),
        Warmer("warmer", 10),
        Completion("completion", 11),
        Segments("segments", 12),
        Translog("translog", 13),
        // 14 was previously used for Suggest
        RequestCache("request_cache", 15),
        Recovery("recovery", 16);

        private final String restName;
        private final int index;

        Flag(final String restName, final int index) {
            this.restName = restName;
            this.index = index;
        }

        public String getRestName() {
            return restName;
        }

        private int getIndex() {
            return index;
        }

    }
}
