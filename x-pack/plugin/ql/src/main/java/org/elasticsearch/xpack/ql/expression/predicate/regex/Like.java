/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import java.util.Objects;

public class Like extends RegexMatch<LikePattern> {

    private final boolean caseInsensitive;

    public Like(Source source, Expression left, LikePattern pattern) {
        this(source, left, pattern, false);
    }

    public Like(Source source, Expression left, LikePattern pattern, boolean caseInsensitive) {
        super(source, left, pattern);
        this.caseInsensitive = caseInsensitive;
    }

    @Override
    protected NodeInfo<Like> info() {
        return NodeInfo.create(this, Like::new, field(), pattern(), caseInsensitive());
    }

    @Override
    protected Like replaceChild(Expression newLeft) {
        return new Like(source(), newLeft, pattern(), caseInsensitive());
    }

    public boolean caseInsensitive() {
        return caseInsensitive;
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(((Like) obj).caseInsensitive(), caseInsensitive());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), caseInsensitive());
    }

}
