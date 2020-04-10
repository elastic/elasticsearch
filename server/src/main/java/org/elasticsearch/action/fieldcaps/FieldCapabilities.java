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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Describes the capabilities of a field optionally merged across multiple indices.
 */
public class FieldCapabilities implements Writeable, ToXContentObject {

    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField SEARCHABLE_FIELD = new ParseField("searchable");
    private static final ParseField AGGREGATABLE_FIELD = new ParseField("aggregatable");
    private static final ParseField INDICES_FIELD = new ParseField("indices");
    private static final ParseField NON_SEARCHABLE_INDICES_FIELD = new ParseField("non_searchable_indices");
    private static final ParseField NON_AGGREGATABLE_INDICES_FIELD = new ParseField("non_aggregatable_indices");
    private static final ParseField META_FIELD = new ParseField("meta");

    private final String name;
    private final String type;
    private final boolean isSearchable;
    private final boolean isAggregatable;

    private final String[] indices;
    private final String[] nonSearchableIndices;
    private final String[] nonAggregatableIndices;

    private final Map<String, Set<String>> meta;

    /**
     * Constructor for a set of indices.
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
     * @param meta Merged metadata across indices.
     */
    public FieldCapabilities(String name, String type,
                             boolean isSearchable, boolean isAggregatable,
                             String[] indices,
                             String[] nonSearchableIndices,
                             String[] nonAggregatableIndices,
                             Map<String, Set<String>> meta) {
        this.name = name;
        this.type = type;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.indices = indices;
        this.nonSearchableIndices = nonSearchableIndices;
        this.nonAggregatableIndices = nonAggregatableIndices;
        this.meta = Objects.requireNonNull(meta);
    }

    FieldCapabilities(StreamInput in) throws IOException {
        this.name = in.readString();
        this.type = in.readString();
        this.isSearchable = in.readBoolean();
        this.isAggregatable = in.readBoolean();
        this.indices = in.readOptionalStringArray();
        this.nonSearchableIndices = in.readOptionalStringArray();
        this.nonAggregatableIndices = in.readOptionalStringArray();
        if (in.getVersion().onOrAfter(Version.V_7_6_0)) {
            meta = in.readMap(StreamInput::readString, i -> i.readSet(StreamInput::readString));
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
        out.writeOptionalStringArray(indices);
        out.writeOptionalStringArray(nonSearchableIndices);
        out.writeOptionalStringArray(nonAggregatableIndices);
        if (out.getVersion().onOrAfter(Version.V_7_6_0)) {
            out.writeMap(meta, StreamOutput::writeString, (o, set) -> o.writeCollection(set, StreamOutput::writeString));
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), type);
        builder.field(SEARCHABLE_FIELD.getPreferredName(), isSearchable);
        builder.field(AGGREGATABLE_FIELD.getPreferredName(), isAggregatable);
        if (indices != null) {
            builder.field(INDICES_FIELD.getPreferredName(), indices);
        }
        if (nonSearchableIndices != null) {
            builder.field(NON_SEARCHABLE_INDICES_FIELD.getPreferredName(), nonSearchableIndices);
        }
        if (nonAggregatableIndices != null) {
            builder.field(NON_AGGREGATABLE_INDICES_FIELD.getPreferredName(), nonAggregatableIndices);
        }
        if (meta.isEmpty() == false) {
            builder.startObject("meta");
            List<Map.Entry<String, Set<String>>> entries = new ArrayList<>(meta.entrySet());
            entries.sort(Comparator.comparing(Map.Entry::getKey)); // provide predictable order
            for (Map.Entry<String, Set<String>> entry : entries) {
                List<String> values = new ArrayList<>(entry.getValue());
                values.sort(String::compareTo); // provide predictable order
                builder.field(entry.getKey(), values);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    public static FieldCapabilities fromXContent(String name, XContentParser parser) throws IOException {
        return PARSER.parse(parser, name);
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<FieldCapabilities, String> PARSER = new ConstructingObjectParser<>(
        "field_capabilities",
        true,
        (a, name) -> new FieldCapabilities(name,
            (String) a[0],
            (boolean) a[1],
            (boolean) a[2],
            a[3] != null ? ((List<String>) a[3]).toArray(new String[0]) : null,
            a[4] != null ? ((List<String>) a[4]).toArray(new String[0]) : null,
            a[5] != null ? ((List<String>) a[5]).toArray(new String[0]) : null,
            a[6] != null ? ((Map<String, Set<String>>) a[6]) : Collections.emptyMap()));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), SEARCHABLE_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), AGGREGATABLE_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), INDICES_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), NON_SEARCHABLE_INDICES_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), NON_AGGREGATABLE_INDICES_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(),
                (parser, context) -> parser.map(HashMap::new, p -> Set.copyOf(p.list())), META_FIELD);
    }

    /**
     * The name of the field.
     */
    public String getName() {
        return name;
    }

    /**
     * Whether this field can be aggregated on all indices.
     */
    public boolean isAggregatable() {
        return isAggregatable;
    }

    /**
     * Whether this field is indexed for search on all indices.
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

    /**
     * Return merged metadata across indices.
     */
    public Map<String, Set<String>> meta() {
        return meta;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldCapabilities that = (FieldCapabilities) o;
        return isSearchable == that.isSearchable &&
            isAggregatable == that.isAggregatable &&
            Objects.equals(name, that.name) &&
            Objects.equals(type, that.type) &&
            Arrays.equals(indices, that.indices) &&
            Arrays.equals(nonSearchableIndices, that.nonSearchableIndices) &&
            Arrays.equals(nonAggregatableIndices, that.nonAggregatableIndices) &&
            Objects.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(name, type, isSearchable, isAggregatable, meta);
        result = 31 * result + Arrays.hashCode(indices);
        result = 31 * result + Arrays.hashCode(nonSearchableIndices);
        result = 31 * result + Arrays.hashCode(nonAggregatableIndices);
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    static class Builder {
        private String name;
        private String type;
        private boolean isSearchable;
        private boolean isAggregatable;
        private List<IndexCaps> indiceList;
        private Map<String, Set<String>> meta;

        Builder(String name, String type) {
            this.name = name;
            this.type = type;
            this.isSearchable = true;
            this.isAggregatable = true;
            this.indiceList = new ArrayList<>();
            this.meta = new HashMap<>();
        }

        /**
         * Collect the field capabilities for an index.
         */
        void add(String index, boolean search, boolean agg, Map<String, String> meta) {
            IndexCaps indexCaps = new IndexCaps(index, search, agg);
            indiceList.add(indexCaps);
            this.isSearchable &= search;
            this.isAggregatable &= agg;
            for (Map.Entry<String, String> entry : meta.entrySet()) {
                this.meta.computeIfAbsent(entry.getKey(), key -> new HashSet<>())
                        .add(entry.getValue());
            }
        }

        List<String> getIndices() {
            return indiceList.stream().map(c -> c.name).collect(Collectors.toList());
        }

        FieldCapabilities build(boolean withIndices) {
            final String[] indices;
            Collections.sort(indiceList, Comparator.comparing(o -> o.name));
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
            final Function<Map.Entry<String, Set<String>>, Set<String>> entryValueFunction = Map.Entry::getValue;
            Map<String, Set<String>> immutableMeta = meta.entrySet().stream()
                    .collect(Collectors.toUnmodifiableMap(
                            Map.Entry::getKey, entryValueFunction.andThen(Set::copyOf)));
            return new FieldCapabilities(name, type, isSearchable, isAggregatable,
                indices, nonSearchableIndices, nonAggregatableIndices, immutableMeta);
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
