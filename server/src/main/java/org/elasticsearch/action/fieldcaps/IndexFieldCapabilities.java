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
import java.util.Collections;
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
        this.name = in.readString();
        this.type = in.readString();
        this.isSearchable = in.readBoolean();
        this.isAggregatable = in.readBoolean();

        // Previously we also stored placeholders for the 'indices' arrays.
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            // Do nothing.
        } else {
            in.readOptionalStringArray();
            in.readOptionalStringArray();
            in.readOptionalStringArray();
        }

        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            meta = in.readMap(StreamInput::readString, StreamInput::readString);
        } else if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
            // Previously we stored meta information as a map of sets.
            Map<String, Set<String>> wrappedMeta = in.readMap(StreamInput::readString, i -> i.readSet(StreamInput::readString));
            meta = wrappedMeta.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().iterator().next()));
        } else {
            meta = Collections.emptyMap();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeString(type);
        out.writeBoolean(isSearchable);
        out.writeBoolean(isAggregatable);

        // Previously we also stored placeholders for the 'indices' arrays.
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeOptionalStringArray(null);
            out.writeOptionalStringArray(null);
            out.writeOptionalStringArray(null);
        }

        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeMap(meta, StreamOutput::writeString, StreamOutput::writeString);
        } else if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
            // Previously we stored meta information as a map of sets.
            Map<String, Set<String>> wrappedMeta = meta.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> Set.of(entry.getValue())));
            out.writeMap(wrappedMeta, StreamOutput::writeString, StreamOutput::writeStringCollection);
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
