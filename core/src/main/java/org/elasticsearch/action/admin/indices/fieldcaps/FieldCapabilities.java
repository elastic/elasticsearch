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

package org.elasticsearch.action.admin.indices.fieldcaps;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Describes the capabilities of a field optionally merged across multiple indices.
 */
public class FieldCapabilities implements Writeable, ToXContent {
    private final String name;
    private final String[] types;
    private final boolean isSearchable;
    private final boolean isAggregatable;

    /**
     * Constructor
     * @param isSearchable Whether this field is indexed for search.
     * @param isAggregatable Whether this field can be aggregated on.
     * @param types The field types associated with the field as an array of unique values.
     */
    FieldCapabilities(String name, boolean isSearchable, boolean isAggregatable, String... types) {
        this.name = name;
        this.types = Objects.requireNonNull(types);
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
    }

    FieldCapabilities(StreamInput in) throws IOException {
        this.name = in.readString();
        this.types = in.readStringArray();
        this.isSearchable = in.readBoolean();
        this.isAggregatable = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(types);
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (types.length == 1) {
            builder.field("types", types[0]);
        } else {
            builder.field("types", types);
        }
        builder.field("searchable", isSearchable);
        builder.field("aggregatable", isAggregatable);
        builder.endObject();
        return builder;
    }

    /**
     * The name of the field
     */
    public String getName() {
        return name;
    }

    /**
     * Whether this field is indexed for search.
     */
    public boolean isAggregatable() {
        return isAggregatable;
    }

    /**
     * Whether this field can be aggregated on.
     */
    public boolean isSearchable() {
        return isSearchable;
    }

    /**
     * The field types associated with the field as an array of unique values.
     */
    public String[] getTypes() {
        return types;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FieldCapabilities that = (FieldCapabilities) o;

        if (isSearchable != that.isSearchable) return false;
        if (isAggregatable != that.isAggregatable) return false;
        return Arrays.equals(types, that.types);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(types);
        result = 31 * result + (isSearchable ? 1 : 0);
        result = 31 * result + (isAggregatable ? 1 : 0);
        return result;
    }
}
