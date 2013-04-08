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
import java.util.EnumSet;

/**
 */
public class CommonStatsFlags implements Streamable {
    private EnumSet<Flag> flags = EnumSet.of(Flag.Docs, Flag.Store, Flag.Indexing, Flag.Get, Flag.Search);
    private String[] types = null;
    private String[] groups = null;

    /**
     * Sets all flags to return all stats.
     */
    public CommonStatsFlags all() {
        flags = EnumSet.allOf(Flag.class);
        types = null;
        groups = null;
        return this;
    }

    /**
     * Clears all stats.
     */
    public CommonStatsFlags clear() {
        flags = EnumSet.noneOf(Flag.class);
        types = null;
        groups = null;
        return this;
    }

    public boolean anySet() {
        return !flags.isEmpty();
    }
    
    public Flag[] getFlags() {
        return flags.toArray(new Flag[flags.size()]);
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
        final long longFlags = in.readLong();
        flags.clear();
        for(Flag flag : Flag.values()) {
            if ((longFlags & (1 << flag.ordinal())) != 0) {
                flags.add(flag);
            }
        }
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
    
    public static enum Flag {
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
        Warmer("warmer");
        
        private final String restName;
        
        Flag(String restName) {
            this.restName = restName;
        }
        
        public String getRestName() {
            return restName;
        }
            
    }
}
