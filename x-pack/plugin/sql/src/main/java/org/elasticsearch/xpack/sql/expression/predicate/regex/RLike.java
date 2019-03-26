/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.gen.processor.Processor;
import org.elasticsearch.xpack.sql.expression.predicate.regex.RegexProcessor.RegexOperation;
import org.elasticsearch.xpack.sql.tree.Source;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class RLike extends RegexMatch<String> {

    public RLike(Source source, Expression value, String pattern) {
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
    
    @Override
    public Boolean fold() {
        Object val = field().fold();
        return RegexOperation.match(val, pattern());
    }

    @Override
    protected Processor makeProcessor() {
        return new RegexProcessor(pattern());
    }
}
