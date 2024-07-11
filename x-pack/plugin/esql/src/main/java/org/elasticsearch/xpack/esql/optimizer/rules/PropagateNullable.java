/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;

import java.util.ArrayList;
import java.util.List;

public class PropagateNullable extends OptimizerRules.PropagateNullable {
    protected Expression nullify(Expression exp, Expression nullExp) {
        if (exp instanceof Coalesce) {
            List<Expression> newChildren = new ArrayList<>(exp.children());
            newChildren.removeIf(e -> e.semanticEquals(nullExp));
            if (newChildren.size() != exp.children().size() && newChildren.size() > 0) { // coalesce needs at least one input
                return exp.replaceChildren(newChildren);
            }
        }
        return Literal.of(exp, null);
    }
}
