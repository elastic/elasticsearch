/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.index;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toMap;

public record EsIndex(
    String name,
    Map<String, EsField> mapping,
    Map<String, IndexMode> indexNameWithModes,
    /** Fields mapped only in some (but *not* all) indices. Since this is only used by the analyzer, it is not serialized. */
    Set<String> partiallyUnmappedFields
) implements Writeable {

    public EsIndex {
        assert name != null;
        assert mapping != null;
        assert partiallyUnmappedFields != null;
    }

    public EsIndex(String name, Map<String, EsField> mapping, Map<String, IndexMode> indexNameWithModes) {
        this(name, mapping, indexNameWithModes, Set.of());
    }

    /**
     * Intended for tests. Returns an index with an empty index mode map.
     */
    public EsIndex(String name, Map<String, EsField> mapping) {
        this(name, mapping, Map.of(), Set.of());
    }

    public static EsIndex readFrom(StreamInput in) throws IOException {
        String name = in.readString();
        Map<String, EsField> mapping = in.readImmutableMap(StreamInput::readString, EsField::readFrom);
        Map<String, IndexMode> indexNameWithModes;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            indexNameWithModes = in.readMap(IndexMode::readFrom);
        } else {
            @SuppressWarnings("unchecked")
            Set<String> indices = (Set<String>) in.readGenericValue();
            assert indices != null;
            indexNameWithModes = indices.stream().collect(toMap(e -> e, e -> IndexMode.STANDARD));
        }
        // partially unmapped fields shouldn't pass the coordinator node anyway, since they are only used by the Analyzer.
        return new EsIndex(name, mapping, indexNameWithModes, Set.of());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name());
        out.writeMap(mapping(), (o, x) -> x.writeTo(out));
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeMap(indexNameWithModes, (o, v) -> IndexMode.writeTo(v, out));
        } else {
            out.writeGenericValue(indexNameWithModes.keySet());
        }
        // partially unmapped fields shouldn't pass the coordinator node anyway, since they are only used by the Analyzer.
    }

    public boolean isPartiallyUnmappedField(String fieldName) {
        return partiallyUnmappedFields.contains(fieldName);
    }

    public Set<String> concreteIndices() {
        return indexNameWithModes.keySet();
    }

    @Override
    public String toString() {
        return name;
    }
}
