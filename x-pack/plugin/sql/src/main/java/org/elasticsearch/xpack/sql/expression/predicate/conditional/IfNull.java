/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.expression.predicate.conditional;

import org.elasticsearch.xpack.sql.expression.Expression;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;

import java.util.Arrays;
import java.util.List;

/**
 * Variant of {@link Coalesce} with two args used by MySQL and ODBC.
 */
public class IfNull extends Coalesce {

    public IfNull(Location location, Expression first, Expression second) {
        this(location, Arrays.asList(first, second));
    }

    private IfNull(Location location, List<Expression> expressions) {
        super(location, expressions);
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new IfNull(location(), newChildren);
    }

    @Override
    protected NodeInfo<IfNull> info() {
        return NodeInfo.create(this, IfNull::new, children().get(0), children().get(1));
    }
}
