/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public abstract sealed class TypeConflictedField extends EsField permits InvalidMappedField, CompactInvalidMappedField {
    public TypeConflictedField(
        String name,
        DataType esDataType,
        Map<String, EsField> properties,
        boolean aggregatable,
        TimeSeriesFieldType timeSeriesFieldType
    ) {
        super(name, esDataType, properties, aggregatable, timeSeriesFieldType);
    }

    public TypeConflictedField(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public EsField getExactField() {
        throw new QlIllegalArgumentException("Field [" + getName() + "] is invalid, cannot access it");
    }

    @Override
    public Exact getExactInfo() {
        return new Exact(false, "Field [" + getName() + "] is invalid, cannot access it");
    }

    private String cachedErrorMessage;

    public String errorMessage() {
        if (cachedErrorMessage == null) {
            cachedErrorMessage = makeErrorMessage();
        }
        return cachedErrorMessage;
    }

    /**
     * Per-source-type indices in which the field appears with that type. Note that {@link CompactInvalidMappedField} caps each set
     * and may include the {@code "..."} sentinel; callers that need a complete index list should use {@link InvalidMappedField}
     * instead.
     */
    public abstract Map<String, Set<String>> getTypesToIndices();

    /** Whether the field is unmapped in at least one index, in which case it's treated as {@link DataType#KEYWORD} where it is unmapped. */
    public abstract boolean isPotentiallyUnmapped();

    /** Source data types observed for this field across all indices. */
    public abstract Set<DataType> types();

    /**
     * A "two-legged PUNK": a field that is mapped to a single type in some indices and unmapped in others. Under
     * {@code unmapped_fields="load"} this is not a genuine type conflict — values are loaded where the field is mapped and are null
     * where it is unmapped.
     */
    public boolean isSingleTypePotentiallyUnmapped() {
        return isPotentiallyUnmapped() && types().size() == 1;
    }

    /**
     * The single mapped source type (e.g. {@code SHORT}). Only valid when {@link #isSingleTypePotentiallyUnmapped()}.
     */
    public DataType singleMappedType() {
        if (isSingleTypePotentiallyUnmapped() == false) {
            throw new IllegalStateException("Unexpected call for a non-single type unmapped field");
        }
        return types().iterator().next();
    }

    /**
     * The single mapped type widened to its ES|QL surface type (e.g. {@code SHORT -> INTEGER}), so the implicit load matches an
     * explicit cast and agrees with the Fork/UnionAll output. Only valid when {@link #isSingleTypePotentiallyUnmapped()}.
     */
    public DataType singleMappedTypeWidened() {
        return singleMappedType().widenSmallNumeric();
    }

    abstract Map<String, Sample> samples();

    record Sample(Collection<String> kept, int total) {}

    private String makeErrorMessage() {
        StringBuilder sb = new StringBuilder();
        var typesToSample = samples();
        boolean includeInsistKeyword = isPotentiallyUnmapped();
        boolean prependInsistKeyword = includeInsistKeyword && samples().containsKey(DataType.KEYWORD.typeName()) == false;
        sb.append("mapped as [");
        sb.append(typesToSample.size() + (prependInsistKeyword ? 1 : 0));
        sb.append("] incompatible types: ");
        boolean first = true;
        if (prependInsistKeyword) {
            first = false;
            sb.append("[keyword] due to loading from _source");
        }
        for (Map.Entry<String, Sample> entry : typesToSample.entrySet()) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append("[").append(entry.getKey()).append("] ");
            sb.append(
                entry.getKey().equals(DataType.KEYWORD.typeName()) && includeInsistKeyword ? "due to loading from _source and in " : "in "
            );
            Sample sample = entry.getValue();
            sb.append(sample.kept());
            int extras = sample.total() - sample.kept().size();
            if (extras > 0) {
                sb.append(" and [").append(extras).append("] other ").append(sample.total() == 4 ? "index" : "indices");
            }
        }
        return sb.toString();
    }
}
