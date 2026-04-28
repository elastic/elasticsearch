/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamOutput;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Representation of field mapped differently across indices; or being potentially unmapped in some, in which case it is treated as
 * {@link DataType#KEYWORD} in the indices where it is unmapped.
 * Used during mapping discovery only.
 * Note that the fields <code>typesToIndices</code> and <code>isPotentiallyUnmapped</code> are not serialized because that information is
 * not required through the cluster, only surviving as long as the Analyser phase of query planning.
 * It is used specifically for the 'union types' and 'unmapped fields' feature in ES|QL.
 */
public class InvalidMappedField extends EsField {

    private static final String ELLIPSIS = "...";
    private static final int MAX_INDICES_PER_TYPE = 3;

    private final String errorMessage;
    private final Map<String, Set<String>> typesToIndices;
    private final boolean isPotentiallyUnmapped;

    public InvalidMappedField(String name, String errorMessage, Map<String, EsField> properties) {
        this(name, errorMessage, properties, Map.of(), false, TimeSeriesFieldType.UNKNOWN);
    }

    public InvalidMappedField(String name, String errorMessage) {
        this(name, errorMessage, new TreeMap<>());
    }

    public InvalidMappedField(String name, Map<String, Set<String>> typesToIndices) {
        this(name, makeErrorMessage(typesToIndices, false), new TreeMap<>(), typesToIndices, false, TimeSeriesFieldType.UNKNOWN);
    }

    /**
     * An {@link InvalidMappedField} is potentially unmapped if at least one index does not contain a mapping for the field, and the user
     * requested we load the values from {@code _source}. In that case, there is (possibly) an additional type conflict since we treat
     * unmapped fields as {@link DataType#KEYWORD}.
     */
    public static InvalidMappedField potentiallyUnmapped(String name, Map<String, Set<String>> typesToIndices) {
        return new InvalidMappedField(
            name,
            makeErrorMessage(typesToIndices, true),
            new TreeMap<>(),
            typesToIndices,
            true,
            TimeSeriesFieldType.UNKNOWN
        );
    }

    /**
     * Memory-frugal variant: stores at most {@value #MAX_INDICES_PER_TYPE} concrete index names per source type (plus the {@value #ELLIPSIS}
     * sentinel when more existed) instead of the full per-type index list. Wide union-typed fields routinely span thousands of indices but
     * the only consumers that need the full list are the legacy index-keyed conversion structures, and they aren't used on transport
     * versions that support {@link CompactMultiTypeEsField}. Truncating here lets the analyzed plan stay small while still producing a good
     * "[a, b, c, ...]" error message: the message itself is rendered from the full input map at construction time and then stored as a
     * string, so we lose only the post-construction ability to enumerate every index.
     *
     * <p>{@code typesToIndices} is not sent over the wire (it only matters during analysis on the coordinator), so the truncation is a
     * coordinator-local memory optimization with no BWC implications.
     */
    public static InvalidMappedField compact(String name, Map<String, Set<String>> typesToIndices) {
        return new InvalidMappedField(
            name,
            makeErrorMessage(typesToIndices, false),
            new TreeMap<>(),
            truncate(typesToIndices),
            false,
            TimeSeriesFieldType.UNKNOWN
        );
    }

    /**
     * {@link #potentiallyUnmapped} counterpart of {@link #compact}.
     */
    public static InvalidMappedField compactPotentiallyUnmapped(String name, Map<String, Set<String>> typesToIndices) {
        return new InvalidMappedField(
            name,
            makeErrorMessage(typesToIndices, true),
            new TreeMap<>(),
            truncate(typesToIndices),
            true,
            TimeSeriesFieldType.UNKNOWN
        );
    }

    private InvalidMappedField(
        String name,
        String errorMessage,
        Map<String, EsField> properties,
        Map<String, Set<String>> typesToIndices,
        boolean isPotentiallyUnmapped,
        TimeSeriesFieldType type
    ) {
        super(name, DataType.UNSUPPORTED, properties, false, type);
        this.errorMessage = errorMessage;
        this.typesToIndices = typesToIndices;
        this.isPotentiallyUnmapped = isPotentiallyUnmapped;
    }

    protected InvalidMappedField(StreamInput in) throws IOException {
        this(
            ((PlanStreamInput) in).readCachedString(),
            in.readString(),
            in.readImmutableMap(StreamInput::readString, EsField::readFrom),
            Map.of(),
            false,
            readTimeSeriesFieldType(in)
        );
    }

    public Set<DataType> types() {
        return typesToIndices.keySet().stream().map(DataType::fromTypeName).collect(Collectors.toSet());
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        ((PlanStreamOutput) out).writeCachedString(getName());
        out.writeString(errorMessage);
        out.writeMap(getProperties(), (o, x) -> x.writeTo(out));
        writeTimeSeriesFieldType(out);
    }

    public String getWriteableName(TransportVersion transportVersion) {
        return "InvalidMappedField";
    }

    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), errorMessage);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            InvalidMappedField other = (InvalidMappedField) obj;
            return Objects.equals(errorMessage, other.errorMessage);
        }

        return false;
    }

    @Override
    public EsField getExactField() {
        throw new QlIllegalArgumentException("Field [" + getName() + "] is invalid, cannot access it");

    }

    @Override
    public Exact getExactInfo() {
        return new Exact(false, "Field [" + getName() + "] is invalid, cannot access it");
    }

    public Map<String, Set<String>> getTypesToIndices() {
        return typesToIndices;
    }

    public boolean isPotentiallyUnmapped() {
        return isPotentiallyUnmapped;
    }

    private static String makeErrorMessage(Map<String, Set<String>> typesToIndices, boolean includeInsistKeyword) {
        StringBuilder errorMessage = new StringBuilder();
        var isInsistKeywordOnlyKeyword = includeInsistKeyword && typesToIndices.containsKey(DataType.KEYWORD.typeName()) == false;
        errorMessage.append("mapped as [");
        errorMessage.append(typesToIndices.size() + (isInsistKeywordOnlyKeyword ? 1 : 0));
        errorMessage.append("] incompatible types: ");
        boolean first = true;
        if (isInsistKeywordOnlyKeyword) {
            first = false;
            errorMessage.append("[keyword] due to loading from _source");
        }
        for (Map.Entry<String, Set<String>> e : typesToIndices.entrySet()) {
            if (first) {
                first = false;
            } else {
                errorMessage.append(", ");
            }
            errorMessage.append("[");
            errorMessage.append(e.getKey());
            errorMessage.append("] ");
            if (e.getKey().equals(DataType.KEYWORD.typeName()) && includeInsistKeyword) {
                errorMessage.append("due to loading from _source and in ");
            } else {
                errorMessage.append("in ");
            }
            if (e.getValue().size() <= 3) {
                errorMessage.append(e.getValue());
            } else {
                errorMessage.append(e.getValue().stream().sorted().limit(3).collect(Collectors.toList()));
                errorMessage.append(" and [" + (e.getValue().size() - 3) + "] other ");
                errorMessage.append(e.getValue().size() == 4 ? "index" : "indices");
            }
        }
        return errorMessage.toString();
    }

    /**
     * Cap each per-type index set at {@value #MAX_INDICES_PER_TYPE} entries, appending the {@value #ELLIPSIS} sentinel iff anything was
     * dropped. The retained entries are picked by sorted order so that the (already truncated) error message and the stored set stay
     * consistent.
     */
    private static Map<String, Set<String>> truncate(Map<String, Set<String>> typesToIndices) {
        Map<String, Set<String>> result = new TreeMap<>();
        for (Map.Entry<String, Set<String>> entry : typesToIndices.entrySet()) {
            Set<String> indices = entry.getValue();
            if (indices.size() <= MAX_INDICES_PER_TYPE) {
                result.put(entry.getKey(), Set.copyOf(indices));
            } else {
                Set<String> truncated = new LinkedHashSet<>(MAX_INDICES_PER_TYPE + 1);
                indices.stream().sorted().limit(MAX_INDICES_PER_TYPE).forEach(truncated::add);
                truncated.add(ELLIPSIS);
                result.put(entry.getKey(), Set.copyOf(truncated));
            }
        }
        return result;
    }
}
