/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.expression;

import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.emptyList;

public abstract class LeafExpression extends Expression {

    protected LeafExpression(Location location) {
        super(location, emptyList());
    }

    public AttributeSet references() {
        return AttributeSet.EMPTY;
    }
}
