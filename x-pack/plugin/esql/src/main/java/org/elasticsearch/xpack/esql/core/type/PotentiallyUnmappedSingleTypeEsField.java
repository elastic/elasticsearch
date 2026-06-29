/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * A variant of {@link PotentiallyUnmappedKeywordEsField} that is mapped to exactly one, non-keyword type where it is mapped, and unmapped
 * in other indices.
 * If an implicit cast from {@link DataType#KEYWORD} to that type exists, it will be auto-cast to that type.
 * Otherwise, it will be loaded as {@code null}, i.e., exactly as it would without {@code UNMAPPED_FIELDS="LOAD"}, which is why it needs to
 * maintain the original (unwidened, with its original properties) mapped {@link EsField}.
 * <br>
 * We treat this as a {@link TypeConflictedField} because it needs to be resolved, either via an implicit cast or null.
 */
public final class PotentiallyUnmappedSingleTypeEsField extends TypeConflictedField {
    private final EsField mappedField;
    private final Set<String> mappedIndices;

    public PotentiallyUnmappedSingleTypeEsField(EsField mappedField, Set<String> mappedIndices) {
        super(mappedField.getName(), DataType.UNSUPPORTED, new TreeMap<>(), false, TimeSeriesFieldType.UNKNOWN);
        this.mappedField = mappedField;
        this.mappedIndices = Set.copyOf(mappedIndices);
    }

    public EsField mappedField() {
        return mappedField;
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PotentiallyUnmappedSingleTypeEsField shouldn't be transported");
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "PotentiallyUnmappedSingleTypeEsField";
    }

    @Override
    public Set<DataType> types() {
        return Set.of(mappedField.getDataType());
    }

    @Override
    public Map<String, Set<String>> getTypesToIndices() {
        return Map.of(mappedField.getDataType().typeName(), mappedIndices);
    }

    @Override
    public boolean isPotentiallyUnmapped() {
        return true;
    }

    @Override
    Map<String, Sample> samples() {
        throw new IllegalStateException("A single-type PUNK should never fail as it always auto-casts or falls back to null.");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), mappedField, mappedIndices);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        PotentiallyUnmappedSingleTypeEsField other = (PotentiallyUnmappedSingleTypeEsField) obj;
        return Objects.equals(mappedField, other.mappedField) && Objects.equals(mappedIndices, other.mappedIndices);
    }
}
