/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class Like extends RegexMatch<LikePattern> {

    public Like(Source source, Expression left, LikePattern pattern) {
        super(source, left, pattern);
    }

    @Override
    protected NodeInfo<Like> info() {
        return NodeInfo.create(this, Like::new, field(), pattern());
    }

    @Override
    protected Like replaceChild(Expression newLeft) {
        return new Like(source(), newLeft, pattern());
    }
}
