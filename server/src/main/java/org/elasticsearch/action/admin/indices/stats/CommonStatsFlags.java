/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
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
        if (in.getVersion().before(Version.V_8_0_0)) {
            in.readStringArray();
        }
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

        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeStringArrayNullable(Strings.EMPTY_ARRAY);
        }
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
        groups = null;
        fieldDataFields = null;
        completionDataFields = null;
        includeSegmentFileSizes = false;
        includeUnloadedSegments = false;
        return this;
    }

    public boolean anySet() {
        return flags.isEmpty() == false;
    }

    public Flag[] getFlags() {
        return flags.toArray(new Flag[flags.size()]);
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
        Recovery("recovery", 16),
        Bulk("bulk", 17),
        Shards("shards", 18);

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
