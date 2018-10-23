/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression.predicate.regex;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.expression.Literal;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

public class RLike extends RegexMatch {

    public RLike(Location location, Expression left, Literal right) {
        super(location, left, right);
    }

    @Override
    protected NodeInfo<RLike> info() {
        return NodeInfo.create(this, RLike::new, left(), (Literal) right());
    }

    @Override
    protected RLike replaceChildren(Expression newLeft, Expression newRight) {
        return new RLike(location(), newLeft, (Literal) newRight);
    }

    @Override
    protected String asString(Expression pattern) {
        return pattern.fold().toString();
    }
}
