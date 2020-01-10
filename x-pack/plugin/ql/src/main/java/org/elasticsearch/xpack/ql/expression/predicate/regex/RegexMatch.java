/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Expressions;
import org.elasticsearch.xpack.ql.expression.Nullability;
import org.elasticsearch.xpack.ql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.type.DataType;

import java.util.Objects;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isStringAndExact;

public abstract class RegexMatch<T> extends UnaryScalarFunction {

    private final T pattern;
    
    protected RegexMatch(Source source, Expression value, T pattern) {
        super(source, value);
        this.pattern = pattern;
    }
    
    public T pattern() {
        return pattern;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        if (pattern() == null) {
            return Nullability.TRUE;
        }
        return field().nullable();
    }

    @Override
    protected TypeResolution resolveType() {
        return isStringAndExact(field(), sourceText(), Expressions.ParamOrdinal.DEFAULT);
    }

    @Override
    public boolean foldable() {
        // right() is not directly foldable in any context but Like can fold it.
        return field().foldable();
    }
    
    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(((RegexMatch<?>) obj).pattern(), pattern());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), pattern());
    }
}
