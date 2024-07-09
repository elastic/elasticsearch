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
import org.elasticsearch.xpack.esql.core.util.PlanStreamInput;

import java.io.IOException;

public class AggregateDoubleMetricAttribute extends FieldAttribute {

    static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Attribute.class,
        "AggregateDoubleMetricAttribute",
        AggregateDoubleMetricAttribute::new
    );

    private final NameId minSubNameId;
    private final NameId maxSubNameId;
    private final NameId sumSubNameId;
    private final NameId valueCountNameId;

    public AggregateDoubleMetricAttribute(
        Source source,
        FieldAttribute parent,
        String name,
        EsField field,
        String qualifier,
        Nullability nullability,
        NameId id,
        boolean synthetic,
        NameId minSubNameId,
        NameId maxSubNameId,
        NameId sumSubNameId,
        NameId valueCountNameId
    ) {
        super(source, parent, name, DataType.AGGREGATE_DOUBLE_METRIC, field, qualifier, nullability, id, synthetic);
        this.minSubNameId = minSubNameId;
        this.maxSubNameId = maxSubNameId;
        this.sumSubNameId = sumSubNameId;
        this.valueCountNameId = valueCountNameId;
    }

    public AggregateDoubleMetricAttribute(StreamInput in) throws IOException {
        super(in);
        this.minSubNameId = NameId.readFrom((StreamInput & PlanStreamInput) in);
        this.maxSubNameId = NameId.readFrom((StreamInput & PlanStreamInput) in);
        this.sumSubNameId = NameId.readFrom((StreamInput & PlanStreamInput) in);
        this.valueCountNameId = NameId.readFrom((StreamInput & PlanStreamInput) in);
    }

    public FieldAttribute getMinSubField() {
        return new AggregateDoubleMetricSubAttribute(source(), name(), minSubNameId, "min");
    }

    public FieldAttribute getMaxSubField() {
        return new AggregateDoubleMetricSubAttribute(source(), name(), maxSubNameId, "max");
    }

    public FieldAttribute getSumSubField() {
        return new AggregateDoubleMetricSubAttribute(source(), name(), sumSubNameId, "sum");
    }

    public FieldAttribute getValueCountSubField() {
        return new AggregateDoubleMetricSubAttribute(source(), name(), valueCountNameId, "value_count");
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
        return new AggregateDoubleMetricAttribute(
            source,
            qualifiedParent,
            name,
            field(),
            qualifier,
            nullability,
            id,
            synthetic,
            minSubNameId,
            maxSubNameId,
            sumSubNameId,
            valueCountNameId
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        minSubNameId.writeTo(out);
        maxSubNameId.writeTo(out);
        sumSubNameId.writeTo(out);
        valueCountNameId.writeTo(out);
    }
}
