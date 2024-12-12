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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class EsIndex implements Writeable {

    private final String name;
    private final Map<String, EsField> mapping;
    private final Map<String, IndexMode> indexNameWithModes;

    public EsIndex(String name, Map<String, EsField> mapping) {
        this(name, mapping, Map.of());
    }

    public EsIndex(String name, Map<String, EsField> mapping, Map<String, IndexMode> indexNameWithModes) {
        assert name != null;
        assert mapping != null;
        this.name = name;
        this.mapping = mapping;
        this.indexNameWithModes = indexNameWithModes;
    }

    public EsIndex(StreamInput in) throws IOException {
        this(in.readString(), in.readImmutableMap(StreamInput::readString, EsField::readFrom), readIndexNameWithModes(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name());
        out.writeMap(mapping(), (o, x) -> x.writeTo(out));
        writeIndexNameWithModes(indexNameWithModes, out);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, IndexMode> readIndexNameWithModes(StreamInput in) throws IOException {
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            return in.readMap(IndexMode::readFrom);
        } else {
            Set<String> indices = (Set<String>) in.readGenericValue();
            assert indices != null;
            return indices.stream().collect(Collectors.toMap(e -> e, e -> IndexMode.STANDARD));
        }
    }

    private static void writeIndexNameWithModes(Map<String, IndexMode> concreteIndices, StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeMap(concreteIndices, (o, v) -> IndexMode.writeTo(v, out));
        } else {
            out.writeGenericValue(concreteIndices.keySet());
        }
    }

    public String name() {
        return name;
    }

    public Map<String, EsField> mapping() {
        return mapping;
    }

    public Map<String, IndexMode> indexNameWithModes() {
        return indexNameWithModes;
    }

    public Set<String> concreteIndices() {
        return indexNameWithModes.keySet();
    }

    @Override
    public String toString() {
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, mapping, indexNameWithModes);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EsIndex other = (EsIndex) obj;
        return Objects.equals(name, other.name)
            && Objects.equals(mapping, other.mapping)
            && Objects.equals(indexNameWithModes, other.indexNameWithModes);
    }
}
