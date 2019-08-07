/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.predicate.regex.RegexProcessor.RegexOperation;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.tree.Source;

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

    @Override
    public Boolean fold() {
        Object val = field().fold();
        return RegexOperation.match(val, pattern().asJavaRegex());
    }

    @Override
    protected Processor makeProcessor() {
        return new RegexProcessor(pattern().asJavaRegex());
    }
}
