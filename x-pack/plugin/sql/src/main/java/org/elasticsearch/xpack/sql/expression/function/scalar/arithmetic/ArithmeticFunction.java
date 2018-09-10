/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.expression.function.scalar.arithmetic.BinaryArithmeticProcessor.BinaryArithmeticOperation;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryNumericFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinition;
import org.elasticsearch.xpack.sql.expression.function.scalar.processor.definition.ProcessorDefinitions;
import org.elasticsearch.xpack.sql.expression.function.scalar.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.function.scalar.script.ParamsBuilder.paramsBuilder;

public abstract class ArithmeticFunction extends BinaryNumericFunction {

    private final BinaryArithmeticOperation operation;

    ArithmeticFunction(Location location, Expression left, Expression right, BinaryArithmeticOperation operation) {
        super(location, left, right);
        this.operation = operation;
    }

    @Override
    public BinaryArithmeticOperation operation() {
        return operation;
    }

    @Override
    public DataType dataType() {
        return DataTypeConversion.commonType(left().dataType(), right().dataType());
    }

    @Override
    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        String op = operation.symbol();
        // escape %
        if (operation == BinaryArithmeticOperation.MOD) {
            op = "%" + op;
        }
        return new ScriptTemplate(format(Locale.ROOT, "(%s) %s (%s)", leftScript.template(), op, rightScript.template()),
                paramsBuilder()
                .script(leftScript.params()).script(rightScript.params())
                .build(), dataType());
    }

    @Override
    protected ProcessorDefinition makeProcessorDefinition() {
        return new BinaryArithmeticProcessorDefinition(location(), this,
                ProcessorDefinitions.toProcessorDefinition(left()),
                ProcessorDefinitions.toProcessorDefinition(right()),
                operation);
    }

    @Override
    public String name() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append(Expressions.name(left()));
        if (!(left() instanceof Literal)) {
            sb.insert(1, "(");
            sb.append(")");
        }
        sb.append(" ");
        sb.append(operation);
        sb.append(" ");
        int pos = sb.length();
        sb.append(Expressions.name(right()));
        if (!(right() instanceof Literal)) {
            sb.insert(pos, "(");
            sb.append(")");
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String toString() {
        return name() + "#" + functionId();
    }
}
