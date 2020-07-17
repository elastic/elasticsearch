/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

public class RLike extends RegexMatch<RLikePattern> {

    public RLike(Source source, Expression value, RLikePattern pattern) {
        super(source, value, pattern);
    }

    @Override
    protected NodeInfo<RLike> info() {
        return NodeInfo.create(this, RLike::new, field(), pattern());
    }

    @Override
    protected RLike replaceChild(Expression newChild) {
        return new RLike(source(), newChild, pattern());
    }
}
