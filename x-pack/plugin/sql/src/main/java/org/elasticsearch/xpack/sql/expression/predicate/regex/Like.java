/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class Like extends RegexMatch {

    private final LikePattern pattern;

    public Like(Location location, Expression left, LikePattern pattern) {
        super(location, left, pattern.asJavaRegex());
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
        return new Like(location(), newLeft, pattern);
    }
}
