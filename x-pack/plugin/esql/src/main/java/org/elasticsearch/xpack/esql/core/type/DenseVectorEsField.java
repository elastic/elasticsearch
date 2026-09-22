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

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Information about a {@code dense_vector} field, including whether it can be searched through Lucene's vector index.
 */
public class DenseVectorEsField extends EsField {

    public static final TransportVersion ESQL_DENSE_VECTOR_FIELD_INDEXED = TransportVersion.fromName("esql_dense_vector_field_indexed");

    private final boolean indexed;

    public DenseVectorEsField(
        String name,
        Map<String, EsField> properties,
        boolean aggregatable,
        boolean isAlias,
        TimeSeriesFieldType timeSeriesFieldType,
        boolean indexed
    ) {
        super(name, DataType.DENSE_VECTOR, properties, aggregatable, isAlias, timeSeriesFieldType);
        this.indexed = indexed;
    }

    DenseVectorEsField(StreamInput in) throws IOException {
        super(in);
        this.indexed = in.readBoolean();
    }

    /** Whether this field can be searched through Lucene's vector index. */
    public boolean isIndexed() {
        return indexed;
    }

    @Override
    public EsField withProperties(Map<String, EsField> newProperties) {
        return new DenseVectorEsField(getName(), newProperties, isAggregatable(), isAlias(), getTimeSeriesFieldType(), indexed);
    }

    @Override
    public void writeContent(StreamOutput out) throws IOException {
        super.writeContent(out);
        if (out.getTransportVersion().supports(ESQL_DENSE_VECTOR_FIELD_INDEXED)) {
            out.writeBoolean(indexed);
        }
    }

    @Override
    public String getWriteableName(TransportVersion transportVersion) {
        return transportVersion.supports(ESQL_DENSE_VECTOR_FIELD_INDEXED) ? "DenseVectorEsField" : "EsField";
    }

    @Override
    public boolean equals(Object o) {
        return super.equals(o) && indexed == ((DenseVectorEsField) o).indexed;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), indexed);
    }
}
