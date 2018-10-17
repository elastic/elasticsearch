/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.expression.predicate.BinaryOperator;
import org.elasticsearch.xpack.sql.expression.predicate.operator.arithmetic.BinaryArithmeticProcessor.BinaryArithmeticOperation;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.DataTypeConversion;

import java.util.Locale;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public abstract class ArithmeticOperation extends BinaryOperator {

    private final BinaryArithmeticOperation operation;

    ArithmeticOperation(Location location, Expression left, Expression right, BinaryArithmeticOperation operation) {
        super(location, left, right, operation.symbol());
        this.operation = operation;
    }
    
    @Override
    protected TypeResolution resolveInputType(DataType inputType) {
        return inputType.isNumeric() ?
                TypeResolution.TYPE_RESOLVED :
                new TypeResolution("'%s' requires a numeric type, received %s", symbol(), inputType.esType);
    }

    @Override
    public ArithmeticOperation swapLeftAndRight() {
        return this;
    }

    @Override
    public DataType dataType() {
        return DataTypeConversion.commonType(left().dataType(), right().dataType());
    }

    @Override
    public Object fold() {
        return operation.apply((Number) left().fold(), (Number) right().fold());
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
    protected Pipe makePipe() {
        return new BinaryArithmeticPipe(location(), this, Expressions.pipe(left()), Expressions.pipe(right()), operation);
    }
}