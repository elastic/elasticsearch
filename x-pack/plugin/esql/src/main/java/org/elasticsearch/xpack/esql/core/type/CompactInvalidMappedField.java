/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

// FIXME(gal, NOCOMMIT) Go over these docs
/**
 * Memory-frugal counterpart to {@link InvalidMappedField}: stores at most {@value #MAX_INDICES_PER_TYPE} concrete index names per source
 * type instead of the full per-type index list. Wide union-typed fields routinely span thousands of indices but the only consumers that
 * need the full list are the legacy index-keyed conversion structures, and they aren't used on transport versions that support
 * {@link CompactMultiTypeEsField}. Truncating here lets the analyzed plan stay small while still producing a good "[a, b, c, ...]" error
 * message: the message itself is rendered from the full input map at construction time and then stored as a string, so we lose only the
 * post-construction ability to enumerate every index.
 *
 * <p>The two classes share the {@link TypeConflictField} interface so consumers (the analyzer, the verifier, type resolution) can branch
 * on it instead of either concrete class. {@link CompactInvalidMappedField} deliberately does <em>not</em> extend
 * {@link InvalidMappedField}: their on-the-wire form is identical (the truncated/full {@code typesToIndices} map is never serialized) so
 * sharing implementation via inheritance would only obscure the fact that they're peer flavors of the same field shape.
 *
 * <p>Wire format matches {@link InvalidMappedField} byte-for-byte and reuses its writeable name, so a {@code CompactInvalidMappedField}
 * round-trips through the wire as a plain {@link InvalidMappedField} on the receiving side. That's fine because {@code typesToIndices}
 * is empty after deserialization anyway, so the truncation no longer matters.
 */
public final class CompactInvalidMappedField extends EsField implements TypeConflictField {
    private static final int MAX_INDICES_PER_TYPE = 3;

    private final String errorMessage;
    private final Map<String, Set<String>> typesToIndices;
    private final boolean isPotentiallyUnmapped;

    public CompactInvalidMappedField(String name, Map<String, Set<String>> typesToIndices) {
        this(name, TypeConflictField.makeErrorMessage(typesToIndices, false), truncate(typesToIndices), false);
    }

    public static CompactInvalidMappedField potentiallyUnmapped(String name, Map<String, Set<String>> typesToIndices) {
        return new CompactInvalidMappedField(
            name,
            TypeConflictField.makeErrorMessage(typesToIndices, true),
            truncate(typesToIndices),
            true
        );
    }

    private CompactInvalidMappedField(
        String name,
        String errorMessage,
        Map<String, Set<String>> typesToIndices,
        boolean isPotentiallyUnmapped
    ) {
        super(name, DataType.UNSUPPORTED, new TreeMap<>(), false, TimeSeriesFieldType.UNKNOWN);
        this.errorMessage = errorMessage;
        this.typesToIndices = typesToIndices;
        this.isPotentiallyUnmapped = isPotentiallyUnmapped;
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("CompactInvalidMappedField shouldn't be transported");
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "InvalidMappedField";
    }

    @Override
    public String errorMessage() {
        return errorMessage;
    }

    @Override
    public Map<String, Set<String>> getTypesToIndices() {
        return typesToIndices;
    }

    @Override
    public boolean isPotentiallyUnmapped() {
        return isPotentiallyUnmapped;
    }

    @Override
    public EsField getExactField() {
        throw new QlIllegalArgumentException("Field [" + getName() + "] is invalid, cannot access it");
    }

    @Override
    public Exact getExactInfo() {
        return new Exact(false, "Field [" + getName() + "] is invalid, cannot access it");
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), errorMessage);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        CompactInvalidMappedField other = (CompactInvalidMappedField) obj;
        return Objects.equals(errorMessage, other.errorMessage);
    }

    /** Cap each per-type index set at {@value #MAX_INDICES_PER_TYPE} entries. */
    private static Map<String, Set<String>> truncate(Map<String, Set<String>> typesToIndices) {
        Map<String, Set<String>> result = new TreeMap<>();
        for (Map.Entry<String, Set<String>> entry : typesToIndices.entrySet()) {
            Set<String> indices = entry.getValue();
            result.put(entry.getKey(), indices.size() <= MAX_INDICES_PER_TYPE ? Set.copyOf(indices) : truncate(indices));
        }
        return result;
    }

    private static @NonNull Set<String> truncate(Set<String> indices) {
        Set<String> truncated = new LinkedHashSet<>(MAX_INDICES_PER_TYPE + 1);
        indices.stream().sorted().limit(MAX_INDICES_PER_TYPE).forEach(truncated::add);
        truncated.add("...");
        return Collections.unmodifiableSet(truncated);
    }
}
