/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.type;

import org.elasticsearch.common.io.stream.StreamOutput;

import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Represents a field that has the same ES data type across all indices but conflicting time-series
 * roles (e.g., {@code DIMENSION} in one index and {@code METRIC} in another).
 * <p>
 * Exists only during analysis and verification - it is not sent to data nodes.
 * The analyzer converts this to an {@link UnsupportedEsField}-backed
 * {@link org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute} via {@code mappingAsAttributes}.
 * <p>
 * Unlike {@link InvalidMappedField}, which carries a {@code typesToIndices} map and can participate in
 * union-type casts (e.g., {@code field::double}), a role conflict cannot be resolved by any cast,
 * so this class is intentionally <em>not</em> a subclass of {@link InvalidMappedField}.
 * <p>
 * Crucially, this class is also not an {@link UnsupportedEsField}. The field-hierarchy walk in
 * {@code IndexResolver.mergedMappings} propagates unsupported status to child fields when it
 * encounters an {@link UnsupportedEsField} parent. By using a distinct type here, subfields with
 * non-conflicting, supported types remain accessible.
 */
public class InvalidMappedTsField extends EsField {

    private final String errorMessage;

    public InvalidMappedTsField(String name, String errorMessage) {
        super(name, DataType.UNSUPPORTED, new TreeMap<>(), false, TimeSeriesFieldType.UNKNOWN);
        this.errorMessage = errorMessage;
    }

    public InvalidMappedTsField(String name, String errorMessage, Map<String, EsField> properties) {
        super(name, DataType.UNSUPPORTED, properties, false, TimeSeriesFieldType.UNKNOWN);
        this.errorMessage = errorMessage;
    }

    @Override
    public void writeContent(StreamOutput out) {
        throw new UnsupportedOperationException("InvalidMappedTsField must never leave the coordinator");
    }

    /**
     * Returns the human-readable error message describing the time-series role conflict.
     */
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
            InvalidMappedTsField other = (InvalidMappedTsField) obj;
            return Objects.equals(errorMessage, other.errorMessage);
        }
        return false;
    }
}
