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

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Describes the capabilities of a field optionally merged across multiple indices.
 */
public class FieldCapabilities implements Writeable, ToXContentObject {
    private final String name;
    private final String type;
    private final boolean isSearchable;
    private final boolean isAggregatable;

    private final String[] indices;
    private final String[] nonSearchableIndices;
    private final String[] nonAggregatableIndices;

    /**
     * Constructor
     * @param name The name of the field.
     * @param type The type associated with the field.
     * @param isSearchable Whether this field is indexed for search.
     * @param isAggregatable Whether this field can be aggregated on.
     */
    FieldCapabilities(String name, String type, boolean isSearchable, boolean isAggregatable) {
        this(name, type, isSearchable, isAggregatable, null, null, null);
    }

    /**
     * Constructor
     * @param name The name of the field
     * @param type The type associated with the field.
     * @param isSearchable Whether this field is indexed for search.
     * @param isAggregatable Whether this field can be aggregated on.
     * @param indices The list of indices where this field name is defined as {@code type},
     *                or null if all indices have the same {@code type} for the field.
     * @param nonSearchableIndices The list of indices where this field is not searchable,
     *                             or null if the field is searchable in all indices.
     * @param nonAggregatableIndices The list of indices where this field is not aggregatable,
     *                               or null if the field is aggregatable in all indices.
     */
    FieldCapabilities(String name, String type,
                      boolean isSearchable, boolean isAggregatable,
                      String[] indices,
                      String[] nonSearchableIndices,
                      String[] nonAggregatableIndices) {
        this.name = name;
        this.type = type;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.indices = indices;
        this.nonSearchableIndices = nonSearchableIndices;
        this.nonAggregatableIndices = nonAggregatableIndices;
    }

    FieldCapabilities(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.isSearchable = in.readBoolean();
        this.isAggregatable = in.readBoolean();
        this.indices = in.readOptionalStringArray();
        this.nonSearchableIndices = in.readOptionalStringArray();
        this.nonAggregatableIndices = in.readOptionalStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);
        out.writeOptionalStringArray(indices);
        out.writeOptionalStringArray(nonSearchableIndices);
        out.writeOptionalStringArray(nonAggregatableIndices);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        builder.field("searchable", isSearchable);
        builder.field("aggregatable", isAggregatable);
        if (indices != null) {
            builder.field("indices", indices);
        }
        if (nonSearchableIndices != null) {
            builder.field("non_searchable_indices", nonSearchableIndices);
        }
        if (nonAggregatableIndices != null) {
            builder.field("non_aggregatable_indices", nonAggregatableIndices);
        }
        builder.endObject();
        return builder;
    }

    /**
     * The name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * Whether this field is indexed for search on all indices.
     */
    public boolean isAggregatable() {
        return isAggregatable;
    }

    /**
     * Whether this field can be aggregated on all indices.
     */
    public boolean isSearchable() {
        return isSearchable;
    }

    /**
     * The type of the field.
     */
    public String getType() {
        return type;
    }

    /**
     * The list of indices where this field name is defined as {@code type},
     * or null if all indices have the same {@code type} for the field.
     */
    public String[] indices() {
        return indices;
    }

    /**
     * The list of indices where this field is not searchable,
     * or null if the field is searchable in all indices.
     */
    public String[] nonSearchableIndices() {
        return nonSearchableIndices;
    }

    /**
     * The list of indices where this field is not aggregatable,
     * or null if the field is aggregatable in all indices.
     */
    public String[] nonAggregatableIndices() {
        return nonAggregatableIndices;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldCapabilities that = (FieldCapabilities) o;

        if (isSearchable != that.isSearchable) return false;
        if (isAggregatable != that.isAggregatable) return false;
        if (!name.equals(that.name)) return false;
        if (!type.equals(that.type)) return false;
        if (!Arrays.equals(indices, that.indices)) return false;
        if (!Arrays.equals(nonSearchableIndices, that.nonSearchableIndices)) return false;
        return Arrays.equals(nonAggregatableIndices, that.nonAggregatableIndices);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + (isSearchable ? 1 : 0);
        result = 31 * result + (isAggregatable ? 1 : 0);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(nonSearchableIndices);
        result = 31 * result + Arrays.hashCode(nonAggregatableIndices);
        return result;
    }

    static class Builder {
        private String name;
        private String type;
        private boolean isSearchable;
        private boolean isAggregatable;
        private List<IndexCaps> indiceList;

        Builder(String name, String type) {
            this.name = name;
            this.type = type;
            this.isSearchable = true;
            this.isAggregatable = true;
            this.indiceList = new ArrayList<>();
        }

        void add(String index, boolean search, boolean agg) {
            IndexCaps indexCaps = new IndexCaps(index, search, agg);
            indiceList.add(indexCaps);
            this.isSearchable &= search;
            this.isAggregatable &= agg;
        }

        FieldCapabilities build(boolean withIndices) {
            final String[] indices;
            /* Eclipse can't deal with o -> o.name, maybe because of
             * https://bugs.eclipse.org/bugs/show_bug.cgi?id=511750 */
            Collections.sort(indiceList, Comparator.comparing((IndexCaps o) -> o.name));
            if (withIndices) {
                indices = indiceList.stream()
                    .map(caps -> caps.name)
                    .toArray(String[]::new);
            } else {
                indices = null;
            }

            final String[] nonSearchableIndices;
            if (isSearchable == false &&
                indiceList.stream().anyMatch((caps) -> caps.isSearchable)) {
                // Iff this field is searchable in some indices AND non-searchable in others
                // we record the list of non-searchable indices
                nonSearchableIndices = indiceList.stream()
                    .filter((caps) -> caps.isSearchable == false)
                    .map(caps -> caps.name)
                    .toArray(String[]::new);
            } else {
                nonSearchableIndices = null;
            }

            final String[] nonAggregatableIndices;
            if (isAggregatable == false &&
                indiceList.stream().anyMatch((caps) -> caps.isAggregatable)) {
                // Iff this field is aggregatable in some indices AND non-searchable in others
                // we keep the list of non-aggregatable indices
                nonAggregatableIndices = indiceList.stream()
                    .filter((caps) -> caps.isAggregatable == false)
                    .map(caps -> caps.name)
                    .toArray(String[]::new);
            } else {
                nonAggregatableIndices = null;
            }
            return new FieldCapabilities(name, type, isSearchable, isAggregatable,
                indices, nonSearchableIndices, nonAggregatableIndices);
        }
    }

    private static class IndexCaps {
        final String name;
        final boolean isSearchable;
        final boolean isAggregatable;

        IndexCaps(String name, boolean isSearchable, boolean isAggregatable) {
            this.name = name;
            this.isSearchable = isSearchable;
            this.isAggregatable = isAggregatable;
        }
    }
}
