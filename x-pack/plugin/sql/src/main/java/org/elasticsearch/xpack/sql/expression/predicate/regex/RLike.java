/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class RLike extends RegexMatch {

    private final String pattern;

    public RLike(Location location, Expression left, String pattern) {
        super(location, left, pattern);
        this.pattern = pattern;
    }

    public String pattern() {
        return pattern;
    }

    @Override
    protected NodeInfo<RLike> info() {
        return NodeInfo.create(this, RLike::new, field(), pattern);
    }

    @Override
    protected RLike replaceChild(Expression newChild) {
        return new RLike(location(), newChild, pattern);
    }
}
