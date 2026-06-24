/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * A field with a single mapped type that is unmapped in at least one index: a partially-unmapped non-keyword field ("PUNK"). Where it is
 * unmapped it reads as {@link DataType#KEYWORD} from {@code _source}, so on top of its single mapped type it carries an implicit KEYWORD leg,
 * making it a {@link TypeConflictedField}. Unlike {@link InvalidMappedField} it retains the original mapped {@link #mappedField()} so the
 * analyzer's null-fallback can restore it verbatim (preserving e.g. aggregatability) when no implicit conversion from KEYWORD exists, so an
 * unloadable PUNK behaves exactly as it would without {@code unmapped_fields="load"}.
 * <p>
 * Keyword PUNKs use {@link PotentiallyUnmappedKeywordEsField} instead, since KEYWORD is loadable from {@code _source} directly (no conflict).
 */
public final class PotentiallyUnmappedSingleTypeField extends TypeConflictedField {
    private final EsField mappedField;
    private final Set<String> mappedIndices;

    /**
     * @param indexDedupCache non-null on clusters new enough to resolve auto-casts via the type-keyed {@link CompactMultiTypeEsField}; there
     *                        the mapped index names are never read (and a single-type PUNK never renders a type-conflict error), so we drop
     *                        them. On older clusters (null) the legacy per-index {@link MultiTypeEsField} builder needs the full set.
     */
    public static PotentiallyUnmappedSingleTypeField of(
        EsField mappedField,
        Set<String> mappedIndices,
        @Nullable Map<Set<String>, Set<String>> indexDedupCache
    ) {
        return new PotentiallyUnmappedSingleTypeField(mappedField, indexDedupCache == null ? Set.copyOf(mappedIndices) : Set.of());
    }

    private PotentiallyUnmappedSingleTypeField(EsField mappedField, Set<String> mappedIndices) {
        super(mappedField.getName(), DataType.UNSUPPORTED, new TreeMap<>(), false, TimeSeriesFieldType.UNKNOWN);
        this.mappedField = mappedField;
        this.mappedIndices = mappedIndices;
    }

    public EsField mappedField() {
        return mappedField;
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("PotentiallyUnmappedSingleTypeField shouldn't be transported");
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "PotentiallyUnmappedSingleTypeField";
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
        // Only feeds type-conflict error messages, which a single-type PUNK never produces: it always auto-casts or falls back to null.
        return Map.of(mappedField.getDataType().typeName(), new Sample(mappedIndices, mappedIndices.size()));
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
        PotentiallyUnmappedSingleTypeField other = (PotentiallyUnmappedSingleTypeField) obj;
        return Objects.equals(mappedField, other.mappedField) && Objects.equals(mappedIndices, other.mappedIndices);
    }
}
