/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.fieldcaps;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.StringLiteralDeduplicator;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Describes the capabilities of a field in a single index.
 */
public class IndexFieldCapabilities implements Writeable {

    private static final StringLiteralDeduplicator typeStringDeduplicator = new StringLiteralDeduplicator();

    private final String name;
    private final String type;
    private final boolean isMetadatafield;
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
    IndexFieldCapabilities(
        String name,
        String type,
        boolean isMetadatafield,
        boolean isSearchable,
        boolean isAggregatable,
        Map<String, String> meta
    ) {

        this.name = name;
        this.type = type;
        this.isMetadatafield = isMetadatafield;
        this.isSearchable = isSearchable;
        this.isAggregatable = isAggregatable;
        this.meta = meta;
    }

    IndexFieldCapabilities(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_7_0)) {
            this.name = in.readString();
            this.type = typeStringDeduplicator.deduplicate(in.readString());
            this.isMetadatafield = in.getVersion().onOrAfter(Version.V_7_13_0) ? in.readBoolean() : false;
            this.isSearchable = in.readBoolean();
            this.isAggregatable = in.readBoolean();
            this.meta = in.readMap(StreamInput::readString, StreamInput::readString);
        } else {
            // Previously we reused the FieldCapabilities class to represent index field capabilities.
            FieldCapabilities fieldCaps = new FieldCapabilities(in);
            this.name = fieldCaps.getName();
            this.type = fieldCaps.getType();
            this.isMetadatafield = fieldCaps.isMetadataField();
            this.isSearchable = fieldCaps.isSearchable();
            this.isAggregatable = fieldCaps.isAggregatable();
            this.meta = fieldCaps.meta()
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().iterator().next()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_7_0)) {
            out.writeString(name);
            out.writeString(type);
            if (out.getVersion().onOrAfter(Version.V_7_13_0)) {
                out.writeBoolean(isMetadatafield);
            }
            out.writeBoolean(isSearchable);
            out.writeBoolean(isAggregatable);
            out.writeMap(meta, StreamOutput::writeString, StreamOutput::writeString);
        } else {
            // Previously we reused the FieldCapabilities class to represent index field capabilities.
            Map<String, Set<String>> wrappedMeta = meta.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> Collections.singleton(entry.getValue())));
            FieldCapabilities fieldCaps = new FieldCapabilities(
                name,
                type,
                isMetadatafield,
                isSearchable,
                isAggregatable,
                null,
                null,
                null,
                wrappedMeta
            );
            fieldCaps.writeTo(out);
        }
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isMetadatafield() {
        return isMetadatafield;
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
        return equalsWithoutName(that) && name.equals(that.name);
    }

    private boolean equalsWithoutName(IndexFieldCapabilities that) {
        return isMetadatafield == that.isMetadatafield
            && isSearchable == that.isSearchable
            && isAggregatable == that.isAggregatable
            && Objects.equals(type, that.type)
            && Objects.equals(meta, that.meta);
    }

    static final class Deduplicator {
        private final Map<String, IndexFieldCapabilities> caches = new HashMap<>();

        IndexFieldCapabilities deduplicate(IndexFieldCapabilities field) {
            final IndexFieldCapabilities existing = caches.putIfAbsent(field.getName(), field);
            if (existing != null) {
                if (existing.equalsWithoutName(field)) {
                    return existing;
                }
                return new IndexFieldCapabilities(
                    existing.getName(),
                    field.getType(),
                    field.isMetadatafield,
                    field.isSearchable,
                    field.isAggregatable,
                    field.meta
                );
            } else {
                return field;
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, isMetadatafield, isSearchable, isAggregatable, meta);
    }
}
