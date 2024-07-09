/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;

import java.io.IOException;
import java.util.Map;

public class AggregateDoubleMetricSubAttribute extends FieldAttribute {

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "AggregateDoubleMetricSubAttribute",
        AggregateDoubleMetricSubAttribute::new
    );

    private final String metric;

    public AggregateDoubleMetricSubAttribute(Source source, String name, String metric) {
        this(source, name, null, metric);
    }

    AggregateDoubleMetricSubAttribute(Source source, String name, NameId nameId, String metric) {
        super(
            source,
            null,
            name + "." + metric,
            "value_count".equals(metric) ? DataType.INTEGER : DataType.DOUBLE,
            new EsField(metric, "value_count".equals(metric) ? DataType.INTEGER : DataType.DOUBLE, Map.of(), true),
            null,
            Nullability.TRUE,
            nameId,
            false
        );
        this.metric = metric;
    }

    AggregateDoubleMetricSubAttribute(
        Source source,
        FieldAttribute parent,
        String name,
        EsField esField,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic,
        String metric
    ) {
        super(source, parent, name, esField.getDataType(), esField, qualifier, nullability, id, synthetic);
        this.metric = metric;
    }

    public AggregateDoubleMetricSubAttribute(StreamInput in) throws IOException {
        super(in);
        metric = in.readString();
    }

    public String getMetric() {
        return metric;
    }

    public String getParentFieldName() {
        return name().substring(0, name().lastIndexOf('.'));
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected Attribute clone(
        Source source,
        String name,
        DataType type,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic
    ) {
        FieldAttribute qualifiedParent = parent() != null ? (FieldAttribute) parent().withQualifier(qualifier) : null;
        return new AggregateDoubleMetricSubAttribute(source, qualifiedParent, name, field(), qualifier, nullability, id, synthetic, metric);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(metric);
    }
}
