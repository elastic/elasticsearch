/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

import java.util.Objects;

public class Like extends RegexMatch {

    private final LikePattern pattern;

    public Like(Source source, Expression left, LikePattern pattern) {
        super(source, left, pattern.asJavaRegex());
        this.pattern = pattern;
    }

    public LikePattern pattern() {
        return pattern;
    }

    @Override
    protected NodeInfo<Like> info() {
        return NodeInfo.create(this, Like::new, field(), pattern);
    }

    @Override
    protected Like replaceChild(Expression newLeft) {
        return new Like(source(), newLeft, pattern);
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj) && Objects.equals(((Like) obj).pattern(), pattern());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), pattern());
    }
}
