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

    public Like(Location location, Expression left, LikePattern right) {
        super(location, left, right);
    }

    @Override
    protected NodeInfo<Like> info() {
        return NodeInfo.create(this, Like::new, left(), pattern());
    }

    public LikePattern pattern() {
        return (LikePattern) right();
    }

    @Override
    protected Like replaceChildren(Expression newLeft, Expression newRight) {
        return new Like(location(), newLeft, (LikePattern) newRight);
    }

    @Override
    protected String asString(Expression pattern) {
        return ((LikePattern) pattern).asJavaRegex();
    }
}
