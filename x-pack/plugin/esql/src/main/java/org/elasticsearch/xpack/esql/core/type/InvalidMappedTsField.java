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
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a field that has the same ES data type across all indices but conflicting time-series
 * roles (e.g., {@code DIMENSION} in one index and {@code METRIC} in another). Unlike
 * {@link InvalidMappedField}, which carries a {@code typesToIndices} map and can participate in
 * union-type casts (e.g., {@code field::double}), a role conflict cannot be resolved by any cast,
 * so this class is intentionally <em>not</em> a subclass of {@link InvalidMappedField}.
 * <p>
 * Crucially, this class is also not an {@link UnsupportedEsField}. The field-hierarchy walk in
 * {@code IndexResolver.mergedMappings} only propagates unsupported status to child fields when it
 * encounters an {@link UnsupportedEsField} parent. By using a distinct type here, subfields with
 * non-conflicting, supported types remain accessible.
 * <p>
 * Used during mapping discovery only. The analyzer converts this to an
 * {@link UnsupportedEsField}-backed {@link org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute}
 * via {@code mappingAsAttributes}.
 * <p>
 * {@link #writeContent(StreamOutput)} deliberately throws {@link UnsupportedOperationException}: serializing an
 * {@link InvalidMappedTsField} is a programming error because these objects must never be sent to data nodes.
 * The {@link StreamInput} constructor is kept solely for backward-compatibility deserialization of any data
 * written before this restriction was enforced.
 */
public class InvalidMappedTsField extends EsField {

    private final String role1;
    private final String role2;

    public InvalidMappedTsField(String name, String role1, String role2, Map<String, EsField> properties) {
        super(name, DataType.UNSUPPORTED, properties, false, TimeSeriesFieldType.UNKNOWN);
        this.role1 = role1;
        this.role2 = role2;
    }

    protected InvalidMappedTsField(StreamInput in) throws IOException {
        this(
            ((PlanStreamInput) in).readCachedString(),
            in.readString(),
            in.readString(),
            in.readImmutableMap(StreamInput::readString, EsField::readFrom)
        );
    }

    @Override
    public void writeContent(StreamOutput out) {
        throw new UnsupportedOperationException(
            "InvalidMappedTsField must be converted to UnsupportedEsField by mappingAsAttributes before the plan is serialized"
        );
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return "InvalidMappedTsField";
    }

    /**
     * Returns the two conflicting time-series roles, e.g. {@code ["dimension", "metric"]}.
     */
    public List<String> getRoles() {
        return List.of(role1, role2);
    }

    /**
     * Returns the human-readable error message describing the time-series role conflict.
     */
    public String errorMessage() {
        return "Cannot use field ["
            + getName()
            + "] with conflicting time-series type mapping: mapped as ["
            + role1
            + "] in some indices and ["
            + role2
            + "] in others";
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), role1, role2);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            InvalidMappedTsField other = (InvalidMappedTsField) obj;
            return Objects.equals(role1, other.role1) && Objects.equals(role2, other.role2);
        }
        return false;
    }
}
