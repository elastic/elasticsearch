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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.EnumSet;

/**
 */
public class CommonStatsFlags implements Streamable, Cloneable {

    public final static CommonStatsFlags ALL = new CommonStatsFlags().all();
    public final static CommonStatsFlags NONE = new CommonStatsFlags().clear();

    private EnumSet<Flag> flags = EnumSet.allOf(Flag.class);
    private String[] types = null;
    private String[] groups = null;
    private String[] fieldDataFields = null;
    private String[] completionDataFields = null;


    /**
     * @param flags flags to set. If no flags are supplied, default flags will be set.
     */
    public CommonStatsFlags(Flag... flags) {
        if (flags.length > 0) {
            clear();
            for (Flag f : flags) {
                this.flags.add(f);
            }
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

    public static CommonStatsFlags readCommonStatsFlags(StreamInput in) throws IOException {
        CommonStatsFlags flags = new CommonStatsFlags();
        flags.readFrom(in);
        return flags;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        long longFlags = 0;
        for (Flag flag : flags) {
            longFlags |= (1 << flag.ordinal());
        }
        out.writeLong(longFlags);

        out.writeStringArrayNullable(types);
        out.writeStringArrayNullable(groups);
        out.writeStringArrayNullable(fieldDataFields);
        out.writeStringArrayNullable(completionDataFields);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        final long longFlags = in.readLong();
        flags.clear();
        for (Flag flag : Flag.values()) {
            if ((longFlags & (1 << flag.ordinal())) != 0) {
                flags.add(flag);
            }
        }
        types = in.readStringArray();
        groups = in.readStringArray();
        fieldDataFields = in.readStringArray();
        completionDataFields = in.readStringArray();
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

    public static enum Flag {
        // Do not change the order of these flags we use
        // the ordinal for encoding! Only append to the end!
        Store("store"),
        Indexing("indexing"),
        Get("get"),
        Search("search"),
        Merge("merge"),
        Flush("flush"),
        Refresh("refresh"),
        FilterCache("filter_cache"),
        IdCache("id_cache"),
        FieldData("fielddata"),
        Docs("docs"),
        Warmer("warmer"),
        Percolate("percolate"),
        Completion("completion"),
        Segments("segments"),
        Translog("translog"),
        Suggest("suggest"),
        QueryCache("query_cache"),
        Recovery("recovery");


        private final String restName;

        Flag(String restName) {
            this.restName = restName;
        }

        public String getRestName() {
            return restName;
        }

    }
}
