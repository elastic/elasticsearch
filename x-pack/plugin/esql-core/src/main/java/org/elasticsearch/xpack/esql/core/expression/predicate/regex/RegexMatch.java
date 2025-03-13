/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.Objects;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isStringAndExact;

public abstract class RegexMatch<T extends StringPattern> extends UnaryScalarFunction {

    private final T pattern;
    private final boolean caseInsensitive;

    protected RegexMatch(Source source, Expression value, T pattern, boolean caseInsensitive) {
        super(source, value);
        this.pattern = pattern;
        this.caseInsensitive = caseInsensitive;
    }

    public T pattern() {
        return pattern;
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
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
        return isStringAndExact(field(), sourceText(), DEFAULT);
    }

    @Override
    public boolean foldable() {
        // right() is not directly foldable in any context but Like can fold it.
        return field().foldable();
    }

    @Override
    public Boolean fold(FoldContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            RegexMatch<?> other = (RegexMatch<?>) obj;
            return caseInsensitive == other.caseInsensitive && Objects.equals(pattern, other.pattern);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), pattern(), caseInsensitive);
    }
}
