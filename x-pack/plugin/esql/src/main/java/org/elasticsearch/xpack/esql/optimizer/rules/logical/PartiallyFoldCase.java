/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.ToString;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;

import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.TEXT;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules.TransformDirection.DOWN;

/**
 * Fold the arms of {@code CASE} statements.
 * {@snippet lang="esql" :
 * EVAL c=CASE(true, foo, bar)
 * }
 * becomes
 * {@snippet lang="esql" :
 * EVAL c=foo
 * }
 */
public final class PartiallyFoldCase extends OptimizerRules.OptimizerExpressionRule<Case> {
    public PartiallyFoldCase() {
        super(DOWN);
    }

    @Override
    protected Expression rule(Case c, LogicalOptimizerContext ctx) {
        Expression folded = c.partiallyFold(ctx.foldCtx());
        // obey the Case.resolveType() and resolveValueType() contract where a TEXT data type is never generated if the folding arm is TEXT
        if (folded.dataType() == TEXT && c.dataType() == KEYWORD) {
            return new ToString(folded.source(), folded, ctx.configuration());
        }
        return folded;
    }
}
