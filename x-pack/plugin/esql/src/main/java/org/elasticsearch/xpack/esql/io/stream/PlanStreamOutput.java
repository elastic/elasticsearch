/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.io.stream.PlanNameRegistry.PlanWriter;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;

import java.io.IOException;
import java.util.function.Function;

/**
 * A customized stream output used to serialize ESQL physical plan fragments. Complements stream
 * output with methods that write plan nodes, Attributes, Expressions, etc.
 */
public final class PlanStreamOutput extends OutputStreamStreamOutput {

    private final PlanNameRegistry registry;

    private final Function<Class<?>, String> nameSupplier;

    public PlanStreamOutput(StreamOutput streamOutput, PlanNameRegistry registry) {
        this(streamOutput, registry, PlanNamedTypes::name);
    }

    public PlanStreamOutput(StreamOutput streamOutput, PlanNameRegistry registry, Function<Class<?>, String> nameSupplier) {
        super(streamOutput);
        this.registry = registry;
        this.nameSupplier = nameSupplier;
    }

    public void writeLogicalPlanNode(LogicalPlan logicalPlan) throws IOException {
        assert logicalPlan.children().size() <= 1;
        writeNamed(LogicalPlan.class, logicalPlan);
    }

    public void writePhysicalPlanNode(PhysicalPlan physicalPlan) throws IOException {
        assert physicalPlan.children().size() <= 1;
        writeNamed(PhysicalPlan.class, physicalPlan);
    }

    public void writeExpression(Expression expression) throws IOException {
        writeNamed(Expression.class, expression);
    }

    public void writeNamedExpression(NamedExpression namedExpression) throws IOException {
        writeNamed(NamedExpression.class, namedExpression);
    }

    public void writeAttribute(Attribute attribute) throws IOException {
        writeNamed(Attribute.class, attribute);
    }

    public void writeOptionalExpression(Expression expression) throws IOException {
        if (expression == null) {
            writeBoolean(false);
        } else {
            writeBoolean(true);
            writeExpression(expression);
        }
    }

    public <T> void writeNamed(Class<T> type, T value) throws IOException {
        String name = nameSupplier.apply(value.getClass());
        @SuppressWarnings("unchecked")
        PlanWriter<T> writer = (PlanWriter<T>) registry.getWriter(type, name);
        writeString(name);
        writer.write(this, value);
    }
}
