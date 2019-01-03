/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Nullability;
import org.elasticsearch.xpack.sql.expression.function.scalar.UnaryScalarFunction;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.predicate.regex.RegexProcessor.RegexOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.type.DataType;

public abstract class RegexMatch extends UnaryScalarFunction {

    private final String pattern;

    protected RegexMatch(Source source, Expression value, String pattern) {
        super(source, value);
        this.pattern = pattern;
    }

    @Override
    public DataType dataType() {
        return DataType.BOOLEAN;
    }

    @Override
    public Nullability nullable() {
        if (pattern == null) {
            return Nullability.TRUE;
        }
        return field().nullable();
    }

    @Override
    public boolean foldable() {
        // right() is not directly foldable in any context but Like can fold it.
        return field().foldable();
    }

    @Override
    public Boolean fold() {
        Object val = field().fold();
        return RegexOperation.match(val, pattern);
    }

    @Override
    protected Processor makeProcessor() {
        return new RegexProcessor(pattern);
    }
}
