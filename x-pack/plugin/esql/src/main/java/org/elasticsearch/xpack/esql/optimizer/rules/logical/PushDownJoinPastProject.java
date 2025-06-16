/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

/**
 * If a {@link Project} is found in the left child of a left {@link Join}, perform it after. Due to requiring the projected attributes
 * later, field extractions can also happen later, making joins cheapter to execute on data nodes.
 * E.g. {@code ... | RENAME field AS otherfield | LOOKUP JOIN lu_idx ON key}
 * becomes {@code ... | LOOKUP JOIN lu_idx ON key | RENAME field AS otherfield }.
 * When a {@code LOOKUP JOIN}'s lookup fields shadow the previous fields, we may need to leave an {@link Eval} in place to assign a
 * temporary name. Assume that {@code field} is a lookup field, then {@code ... | RENAME field AS otherfield | LOOKUP JOIN lu_idx ON key}
 * becomes something like {@code ... | EVAL $$field = field | LOOKUP JOIN lu_idx ON key | RENAME $$field AS otherfield}.
 * Leaving {@code EVAL $$field = field} in place of the original projection, rather than a Project, avoids infinite loops.
 */
public final class PushDownJoinPastProject extends OptimizerRules.OptimizerRule<Join> {
    @Override
    protected LogicalPlan rule(Join join) {
        if (join.left() instanceof Project project && join.config().type() == JoinTypes.LEFT)
        {
            // 1. Propagate any renames into the Join, as we will remove the upstream Project.
            //    E.g. `RENAME field AS key | LOOKUP JOIN idx ON key` -> `LOOKUP JOIN idx ON field | ...`
            // 2. Construct the downstream Project using the Join's output.
            //    Use trivial aliases for now, so we can easily update the expressions.
            //    This is like adding a `RENAME field1 AS field1, field2 AS field2, ...` after the Join, where we can easily change the
            //    referenced field names to deal with name conflicts.
            // 3. Propagate any renames from the upstream Project into the new downstream Project.
            // 4. Look for name conflicts: any name shadowed by the `LOOKUP JOIN` that was used in the original Project needs to be
            //    aliased temporarily. Add an Eval upstream from the `LOOKUP JOIN` and propagate its renames into the new downstream
            //    Project.
            // 5. Remove any trivial aliases from the new downstream Project - `RENAME field AS field` just becomes (the equivalent of)
            //    `KEEP field`.
        }

        return join;
    }
}
