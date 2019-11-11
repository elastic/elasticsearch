/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.expression;

import org.elasticsearch.xpack.eql.EqlIllegalArgumentException;
import org.elasticsearch.xpack.eql.tree.Source;

import java.util.List;

import static java.util.Collections.emptyList;

public abstract class LeafExpression extends Expression {

    public LeafExpression(Source source) {
        super(source, emptyList());
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        throw new EqlIllegalArgumentException("{} doesn't have any children to replace", getClass().getSimpleName());
    }
}