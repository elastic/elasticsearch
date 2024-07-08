/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;

public class Like extends RegexMatch<LikePattern> {

    public Like(Source source, Expression left, LikePattern pattern) {
        this(source, left, pattern, false);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException();
    }

    public Like(Source source, Expression left, LikePattern pattern, boolean caseInsensitive) {
        super(source, left, pattern, caseInsensitive);
    }

    @Override
    protected NodeInfo<Like> info() {
        return NodeInfo.create(this, Like::new, field(), pattern(), caseInsensitive());
    }

    @Override
    protected Like replaceChild(Expression newLeft) {
        return new Like(source(), newLeft, pattern(), caseInsensitive());
    }

}
