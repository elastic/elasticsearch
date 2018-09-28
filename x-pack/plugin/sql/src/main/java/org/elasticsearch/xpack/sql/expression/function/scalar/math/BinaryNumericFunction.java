/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.function.scalar.math;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Expressions;
import org.elasticsearch.xpack.sql.expression.function.scalar.BinaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.function.scalar.math.BinaryMathProcessor.BinaryMathOperation;
import org.elasticsearch.xpack.sql.expression.gen.pipeline.Pipe;
import org.elasticsearch.xpack.sql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;

import java.util.Locale;
import java.util.Objects;

import static java.lang.String.format;
import static org.elasticsearch.xpack.sql.expression.gen.script.ParamsBuilder.paramsBuilder;

public abstract class BinaryNumericFunction extends BinaryScalarFunction {

    private final BinaryMathOperation operation;

    protected BinaryNumericFunction(Location location, Expression left, Expression right, BinaryMathOperation operation) {
        super(location, left, right);
        this.operation = operation;
    }

    @Override
    public DataType dataType() {
        return DataType.DOUBLE;
    }

    @Override
    protected TypeResolution resolveType() {
        if (!childrenResolved()) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = resolveInputType(left().dataType());

        if (resolution == TypeResolution.TYPE_RESOLVED) {
            return resolveInputType(right().dataType());
        }
        return resolution;
    }

    protected TypeResolution resolveInputType(DataType inputType) {
        return inputType.isNumeric() ?
                TypeResolution.TYPE_RESOLVED :
                new TypeResolution("'%s' requires a numeric type, received %s", mathFunction(), inputType.esType);
    }

    @Override
    public Object fold() {
        return operation.apply((Number) left().fold(), (Number) right().fold());
    }

    @Override
    protected Pipe makePipe() {
        return new BinaryMathPipe(location(), this, Expressions.pipe(left()), Expressions.pipe(right()), operation);
    }

    @Override
    protected ScriptTemplate asScriptFrom(ScriptTemplate leftScript, ScriptTemplate rightScript) {
        return new ScriptTemplate(format(Locale.ROOT, "Math.%s(%s,%s)", mathFunction(), leftScript.template(), rightScript.template()),
                paramsBuilder()
                    .script(leftScript.params()).script(rightScript.params())
                    .build(), dataType());
    }

    protected String mathFunction() {
        return getClass().getSimpleName().toLowerCase(Locale.ROOT);
    }

    @Override
    public int hashCode() {
        return Objects.hash(left(), right(), operation);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        BinaryNumericFunction other = (BinaryNumericFunction) obj;
        return Objects.equals(other.left(), left())
            && Objects.equals(other.right(), right())
                && Objects.equals(other.operation, operation);
    }
}