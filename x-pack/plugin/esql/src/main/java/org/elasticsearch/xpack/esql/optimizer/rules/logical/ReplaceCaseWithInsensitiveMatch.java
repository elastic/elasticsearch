/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.predicate.logical.Not;
import org.elasticsearch.xpack.esql.core.expression.predicate.operator.comparison.BinaryComparison;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ChangeCase;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.NotEquals;

public class ReplaceCaseWithInsensitiveMatch extends OptimizerRules.OptimizerExpressionRule<BinaryComparison> {

    public ReplaceCaseWithInsensitiveMatch() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected Expression rule(BinaryComparison bc) {
        Expression e = bc;
        if (bc.left() instanceof ChangeCase changeCase && bc.right().foldable()) {
            if (bc instanceof Equals) {
                e = replaceChangeCase(bc, changeCase);
            } else if (bc instanceof NotEquals) { // not actually used currently, `!=` is built as `NOT(==)` already
                e = new Not(bc.source(), replaceChangeCase(bc, changeCase));
            }
        }
        return e;
    }

    private static Expression replaceChangeCase(BinaryComparison bc, ChangeCase changeCase) {
        var foldedRight = BytesRefs.toString(bc.right().fold());
        return changeCase.caseType().matchesCase(foldedRight)
            ? new InsensitiveEquals(bc.source(), changeCase.field(), bc.right())
            : Literal.FALSE;

    }
}
