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
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

public abstract sealed class TypeConflictedField extends EsField permits InvalidMappedField, CompactInvalidMappedField,
    PotentiallyUnmappedSingleTypeEsField {
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
     * The field's types as the legs of a union column, in the one order everything agrees on.
     * <p>
     * The order is a function of the types themselves, by type name, which is what makes it canonical. It is not recomputed
     * anywhere, though: this method has one production caller, the analyzer rule that builds the {@link UnionEsField}, and from
     * there the list is carried - with the plan, with the block's own leg element types, and by name into the response's
     * {@code original_types}. A value's tag indexes that one carried list, so the order a reader sees is the order the field
     * holds rather than one computed a second time and hoped to match.
     * </p>
     * <p>
     * The types are widened first. A {@code float} or a {@code short} is widened on load and never reaches a
     * value, so a leg declared as one could not be rendered.
     * </p>
     * <p>
     * A field unmapped in some of its indices gets a {@code keyword} leg on top of {@link #types()}, because that is what those
     * indices load it as from {@code _source} - the same leg {@link #errorMessage()} already counts. Whether they load at all is
     * the query's {@code unmapped_fields} mode to decide, which this cannot see, so the leg is always declared: one that turns out
     * to hold nothing costs a null block, while a missing one would have nowhere to put a value.
     * </p>
     */
    public List<DataType> unionLegTypes() {
        Stream<DataType> types = types().stream();
        if (isPotentiallyUnmapped()) {
            types = Stream.concat(types, Stream.of(DataType.KEYWORD));
        }
        return types.map(DataType::widenSmallNumeric).distinct().sorted(Comparator.comparing(DataType::typeName)).toList();
    }

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
     * explicit cast and agrees with the {@code MergePlan} output. Only valid when {@link #isSingleTypePotentiallyUnmapped()}.
     */
    public DataType singleMappedTypeWidened() {
        return singleMappedType().widenSmallNumeric();
    }

    abstract Map<String, Sample> samples();

    record Sample(Collection<String> kept, int total) {}

    private String makeErrorMessage() {
        StringBuilder sb = new StringBuilder();
        var typesToSample = samples();
        boolean includeUnmappedKeyword = isPotentiallyUnmapped();
        boolean prependUnmappedKeyword = includeUnmappedKeyword && samples().containsKey(DataType.KEYWORD.typeName()) == false;
        sb.append("mapped as [");
        sb.append(typesToSample.size() + (prependUnmappedKeyword ? 1 : 0));
        sb.append("] incompatible types: ");
        boolean first = true;
        if (prependUnmappedKeyword) {
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
                entry.getKey().equals(DataType.KEYWORD.typeName()) && includeUnmappedKeyword ? "due to loading from _source and in " : "in "
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
