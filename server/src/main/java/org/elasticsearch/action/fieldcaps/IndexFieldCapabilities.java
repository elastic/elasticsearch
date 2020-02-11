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

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Describes the capabilities of a field in a single index.
 */
public class IndexFieldCapabilities implements Writeable {

    private final String name;
    private final String type;
    private final boolean isSearchable;
    private final boolean isAggregatable;
    private final Map<String, String> meta;

    /**
     * @param name The name of the field.
     * @param type The type associated with the field.
     * @param isSearchable Whether this field is indexed for search.
     * @param isAggregatable Whether this field can be aggregated on.
     * @param meta Metadata about the field.
     */
    IndexFieldCapabilities(String name, String type,
                           boolean isSearchable, boolean isAggregatable,
                           Map<String, String> meta) {

        this.name = name;
        this.type = type;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.meta = meta;
    }

    IndexFieldCapabilities(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            this.name = in.readString();
            this.type = in.readString();
            this.isSearchable = in.readBoolean();
            this.isAggregatable = in.readBoolean();
            this.meta = in.readMap(StreamInput::readString, StreamInput::readString);
        } else {
            // Previously we reused the FieldCapabilities class to represent index field capabilities.
            FieldCapabilities fieldCaps = new FieldCapabilities(in);
            this.name = fieldCaps.getName();
            this.type = fieldCaps.getType();
            this.isSearchable = fieldCaps.isSearchable();
            this.isAggregatable = fieldCaps.isAggregatable();
            this.meta = fieldCaps.meta().entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().iterator().next()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeString(name);
            out.writeString(type);
            out.writeBoolean(isSearchable);
            out.writeBoolean(isAggregatable);
            out.writeMap(meta, StreamOutput::writeString, StreamOutput::writeString);
        } else {
            // Previously we reused the FieldCapabilities class to represent index field capabilities.
            Map<String, Set<String>> wrappedMeta = meta.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> Set.of(entry.getValue())));
            FieldCapabilities fieldCaps = new FieldCapabilities(name, type, isSearchable, isAggregatable, null, null, null, wrappedMeta);
            fieldCaps.writeTo(out);
        }
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isAggregatable() {
        return isAggregatable;
    }

    public boolean isSearchable() {
        return isSearchable;
    }

    public Map<String, String> meta() {
        return meta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IndexFieldCapabilities that = (IndexFieldCapabilities) o;
        return isSearchable == that.isSearchable &&
            isAggregatable == that.isAggregatable &&
            Objects.equals(name, that.name) &&
            Objects.equals(type, that.type) &&
            Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, isSearchable, isAggregatable, meta);
    }
}
