/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public abstract class WildcardLike extends RegexMatch<WildcardPattern> {

    public WildcardLike(Source source, Expression left, WildcardPattern pattern) {
        this(source, left, pattern, false);
    }

    public WildcardLike(Source source, Expression left, WildcardPattern pattern, boolean caseInsensitive) {
        super(source, left, pattern, caseInsensitive);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
    }

}
