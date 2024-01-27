/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator.predicate.operator.regex;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.regex.RLikePattern;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

public class RLike extends org.elasticsearch.xpack.ql.expression.predicate.regex.RLike {
    public RLike(Source source, Expression value, RLikePattern pattern) {
        super(source, value, pattern);
    }

    public RLike(Source source, Expression field, RLikePattern rLikePattern, boolean caseInsensitive) {
        super(source, field, rLikePattern, caseInsensitive);
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.ql.expression.predicate.regex.RLike> info() {
        return NodeInfo.create(this, RLike::new, field(), pattern(), caseInsensitive());
    }

    @Override
    protected RLike replaceChild(Expression newChild) {
        return new RLike(source(), newChild, pattern(), caseInsensitive());
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT);
    }
}
