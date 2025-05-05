/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Pushdown a LEFT {@link Join} past a {@link Project} in its left child.
 *
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
 * There can be name conflicts, where a field added to the left output via the {@link Join} shadows a field that was used to perform a
 * rename before. In such cases, an {@link Eval} node has to remain upstream from the {@link Join} to perform the rename. But we still get
 * the benefit of precise field extractions as long as the {@link Project} can bubble downstream.
 *
 * E.g.
 * \_Join[LEFT,[language_code],[language_code],[language_code]]
 *   |_Project[[language_code, language_name{f}#1, language_name{f}#1 AS foo]] <- conflict: language_name#1 shadowed by #2 but foo needs it
 *   | \_EsRelation[test][language_code, language_name{f}#1]
 *   \_EsRelation[languages_lookup][LOOKUP][language_code, language_name{f}#2]
 *
 * becomes
 * \_Project[[language_code, foo, language_name{f}#2]]
 * \_Join[LEFT,[language_code],[language_code],[language_code]]
 *   |_Eval[[language_name{f}#1 AS foo]]                        <- we still need language_name#1 for foo
 *   | \_EsRelation[test][language_code, language_name{f}#1]
 *   \_EsRelation[languages_lookup][LOOKUP][language_code, language_name{f}#2]
 */
public final class PushDownJoinPastProject extends OptimizerRules.OptimizerRule<Join> {
    @Override
    protected LogicalPlan rule(Join join) {
        if (join.left() instanceof Project projectChild && JoinTypes.LEFT.equals(join.config().type())) {
            // Collect the output of the Join; we'll construct the downstream Project from this.
            List<Attribute> finalOutput = join.output();

            // Find out which fields the Join adds to the fields of the left input.
            Set<String> rightOutputNames = join.rightOutputFields().stream().map(NamedExpression::name).collect(Collectors.toSet());
            // Collect the renames from the Project into an AttributeMap to resolve the Join's references.
            // TODO

            // Also resolve the final projections; some of these can resolve to an attribute that is shadowed by the right fields.
            // For such fields, we need to create a synthetic attribute with a temporary name and add an Eval upstream from the Join to
            // preserve these attributes.

            // TODO: here
            return join;
        }

        return join;

    }
}
