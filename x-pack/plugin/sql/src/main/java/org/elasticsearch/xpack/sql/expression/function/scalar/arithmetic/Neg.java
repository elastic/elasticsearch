/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.NamedExpression;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.UnaryArithmeticProcessor.UnaryArithmeticOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.UnaryProcessorDefinition;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;

/**
 * Negation function (@{code -x}).
 */
public class Neg extends UnaryScalarFunction {
    public Neg(Location location, Expression field) {
        super(location, field);
    }

    @Override
    protected NodeInfo<Neg> info() {
        return NodeInfo.create(this, Neg::new, field());
    }

    @Override
    protected UnaryScalarFunction replaceChild(Expression newChild) {
        return new Neg(location(), newChild);
    }

    @Override
    protected TypeResolution resolveType() {
        return Expressions.typeMustBeNumeric(field());
    }

    @Override
    public Object fold() {
        return Arithmetics.negate((Number) field().fold());
    }

    @Override
    public DataType dataType() {
        return field().dataType();
    }

    @Override
    public String name() {
        return "-" + (field() instanceof NamedExpression && field().resolved() ? Expressions.name(field()) : field().toString());
    }

    @Override
    protected String formatScript(String template) {
        // Painless supports negating (and hopefully its corner cases)
        return super.formatScript("-" + template);
    }

    @Override
    protected ProcessorDefinition makeProcessorDefinition() {
        return new UnaryProcessorDefinition(location(), this, ProcessorDefinitions.toProcessorDefinition(field()),
                new UnaryArithmeticProcessor(UnaryArithmeticOperation.NEGATE));
    }
}
