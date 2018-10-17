/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.comparison;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.sql.expression.predicate.operator.comparison.BinaryComparisonProcessor.BinaryComparisonOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

// marker class to indicate operations that rely on values
public abstract class BinaryComparison extends BinaryOperator {

    private final BinaryComparisonOperation operation;

    public BinaryComparison(Location location, Expression left, Expression right, BinaryComparisonOperation operation) {
        super(location, left, right, operation.symbol());
        this.operation = operation;
    }

    @Override
    protected TypeResolution resolveInputType(DataType inputType) {
        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        String op = operation.symbol();
        return new ScriptTemplate(format(Locale.ROOT, "(%s) %s (%s)", leftScript.template(), op, rightScript.template()),
                paramsBuilder()
                .script(leftScript.params()).script(rightScript.params())
                .build(), dataType());
    }

    @Override
    protected Pipe makePipe() {
        return new BinaryComparisonPipe(location(), this, Expressions.pipe(left()), Expressions.pipe(right()), operation);
    }

    @Override
    public Object fold() {
        return operation.apply(left().fold(), right().fold());
    }

    @Override
    protected Expression canonicalize() {
        return left().hashCode() > right().hashCode() ? swapLeftAndRight() : this;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    public static Integer compare(Object left, Object right) {
        return Comparisons.compare(left, right);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(left());
        sb.append(" ");
        sb.append(symbol());
        sb.append(" ");
        sb.append(right());
        return sb.toString();
    }
}
