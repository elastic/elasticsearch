/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;

/**
 * This is different from e.g. {@link PushDownEnrich} because a {@link Join} is not a
 * {@link org.elasticsearch.xpack.esql.plan.GeneratingPlan} as we cannot assign arbitrary names to the "added" columns, which is required
 * to solve name conflicts while pushing down. (We could assign arbitrary qualifiers if we had them, however, to achieve the same.)
 *
 * This will turn
 * \_Join[LEFT,[language_code],[language_code],[language_code]]
 *   |_Project[[language_code]]
 *   | \_EsRelation[test][other_field, language_code]
 *   \_EsRelation[languages_lookup][LOOKUP][language_code, language_name]
 *
 * into
 * \_Project[[language_code, language_name]]
 *   \_Join[LEFT,[language_code],[language_code],[language_code]]           <- LOOKUP JOIN languages_lookup ON language_code
 *     | \_EsRelation[test][other_field, language_code]
 *     \_EsRelation[languages_lookup][LOOKUP][language_code, language_name]
 *
 * which allows multiple {@link Project}s to be combined downstream so that determining which fields actually matter and need to be
 * extraced becomes more precise.
 *
 * In case of name conflicts, a {@link Project} node has to remain upstream from the {@link Join} to perform a renaming; but we still get
 * the benefit of precise field extractions as long as the {@link Project} can bubble downstream.
 *
 * E.g.
 * \_Join[LEFT,[language_code],[language_code],[language_code]]
 *   |_Project[[language_code, language_name{f}#1, language_name{f}#1 AS foo]] <- conflict: language_name#1 shadowed by #2 but foo needs it
 *   | \_EsRelation[test][language_code, language_name{f}#1]
 *   \_EsRelation[languages_lookup][LOOKUP][language_code, language_name{f}#2]
 *
 * becomes
 * \_Project[[language_code, $$temp$name AS foo, language_name{f}#2]]
 * \_Join[LEFT,[language_code],[language_code],[language_code]]
 *   |_Project[[language_code, language_name{f}#1 AS $$temp$name]]             <- we still need language_name#1 for foo
 *   | \_EsRelation[test][language_code, language_name{f}#1]
 *   \_EsRelation[languages_lookup][LOOKUP][language_code, language_name{f}#2]
 */
public final class PushDownJoinPastProject extends OptimizerRules.OptimizerRule<Join> {
    @Override
    protected LogicalPlan rule(Join join) {
        // TODO: here
        return join;
    }
}
