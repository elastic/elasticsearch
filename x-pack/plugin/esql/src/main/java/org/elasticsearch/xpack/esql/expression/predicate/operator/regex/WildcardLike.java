/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.regex;

import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.ql.tree.NodeInfo;
import org.elasticsearch.xpack.ql.tree.Source;

import static org.elasticsearch.xpack.ql.expression.TypeResolutions.ParamOrdinal.DEFAULT;
import static org.elasticsearch.xpack.ql.expression.TypeResolutions.isString;

public class WildcardLike extends org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardLike {
    public WildcardLike(Source source, Expression left, WildcardPattern pattern, boolean caseInsensitive) {
        super(source, left, pattern, caseInsensitive);
    }

    @Override
    protected NodeInfo<org.elasticsearch.xpack.ql.expression.predicate.regex.WildcardLike> info() {
        return NodeInfo.create(this, WildcardLike::new, field(), pattern(), caseInsensitive());
    }

    @Override
    protected WildcardLike replaceChild(Expression newLeft) {
        return new WildcardLike(source(), newLeft, pattern(), caseInsensitive());
    }

    @Override
    protected TypeResolution resolveType() {
        return isString(field(), sourceText(), DEFAULT);
    }
}
