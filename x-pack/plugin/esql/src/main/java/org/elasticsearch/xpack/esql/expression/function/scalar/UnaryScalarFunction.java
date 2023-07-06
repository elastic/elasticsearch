/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.TypeResolutions;
import org.elasticsearch.xpack.ql.expression.function.scalar.ScalarFunction;
import org.elasticsearch.xpack.ql.expression.gen.script.ScriptTemplate;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Arrays;
import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isNumeric;

public abstract class UnaryScalarFunction extends ScalarFunction {
    private final Expression field;

    public UnaryScalarFunction(Source source, Expression field) {
        super(source, Arrays.asList(field));
        this.field = field;
    }

    @Override
    protected Expression.TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new Expression.TypeResolution("Unresolved children");
        }

        return isNumeric(field, sourceText(), TypeResolutions.ParamOrdinal.DEFAULT);
    }

    @Override
    public boolean foldable() {
        return field.foldable();
    }

    public final Expression field() {
        return field;
    }

    @Override
    public DataType dataType() {
        return field.dataType();
    }

    @Override
    public final int hashCode() {
        return Objects.hash(field);
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == null || obj.getClass() != getClass()) {
            return false;
        }
        UnaryScalarFunction other = (UnaryScalarFunction) obj;
        return Objects.equals(other.field, field);
    }

    @Override
    public final ScriptTemplate asScript() {
        throw new UnsupportedOperationException("functions do not support scripting");
    }
}
