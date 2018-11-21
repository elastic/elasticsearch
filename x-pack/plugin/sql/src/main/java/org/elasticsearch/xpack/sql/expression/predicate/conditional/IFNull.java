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

public class IFNull extends Coalesce {

    public IFNull(Location location, Expression first, Expression second) {
        super(location, Arrays.asList(first, second));
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new IFNull(location(), newChildren.get(0), newChildren.get(1));
    }

    @Override
    protected NodeInfo<IFNull> info() {
        return NodeInfo.create(this, IFNull::new, children().get(0), children().get(1));
    }
}
