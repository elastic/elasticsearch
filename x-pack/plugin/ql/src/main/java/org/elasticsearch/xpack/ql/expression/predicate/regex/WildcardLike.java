/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class WildcardLike extends RegexMatch<WildcardPattern> {

    public WildcardLike(Source source, Expression left, WildcardPattern pattern) {
        this(source, left, pattern, false);
    }

    public WildcardLike(Source source, Expression left, WildcardPattern pattern, boolean caseInsensitive) {
        super(source, left, pattern, caseInsensitive);
    }

    @Override
    protected NodeInfo<WildcardLike> info() {
        return NodeInfo.create(this, WildcardLike::new, field(), pattern(), caseInsensitive());
    }

    @Override
    protected WildcardLike replaceChild(Expression newLeft) {
        return new WildcardLike(source(), newLeft, pattern(), caseInsensitive());
    }

}
