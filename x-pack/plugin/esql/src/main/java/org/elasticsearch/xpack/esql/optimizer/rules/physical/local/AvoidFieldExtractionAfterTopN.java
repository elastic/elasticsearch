/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;
import java.util.Optional;

/**
 * Modify a {@link ProjectExec} that follows a {@link TopNExec} such that it tries to minimize field extraction on the data driver.
 *
 * Consider the following query:
 * <pre>
 * FROM index | WHERE x > 10 | SORT foo | LIMIT 10 | KEEP bar
 * </pre>
 * If we can delay materializing {@code bar} until the node-reduce driver has finished its own TopN, we can reduce the amount of data we
 * read from the index.
 *
 * The basic strategy here is to "cut off" the operation right after the last top n, and perform all the removed operations on the
 * reduce-side, so the data-side top n operations "feed into" the reduce-side one. Ideally, we would just take the top-most
 * {@link TopNExec}, but unfortunately that doesn't quite work: the top n might be pushed down to the source in {@link PushTopNToSource},
 * then the output schema might change (the filter might also be pushed down, so no {@code x} will be output). To solve this, we add a
 * {@link ProjectExec} to ensure that the output schema of the data-side plan remains consistent with the expectations of the reduce-side
 * plan (note that while performing the reduce-side plan we have no way of knowing if a pushdown is possible or not, since we don't have
 * access to the source's capabilities).
 *
 * So for the aforementioned query, we would go from (roughly) this plan:
 * <pre>
 *  ProjectExec [bar]
 *  └── TopNExec [foo, limit=10] (this will output _doc, foo, and x)
 *      └── FilterExec [x > 10]
 *          └── EsQueryExec [index]
 *  </pre>
 *  Into this:
 *  <pre>
 *  ProjectExec [_doc, foo, x]
 *  └── TopNExec [foo, limit=10]
 *      └── FilterExec [x > 10]
 *          └── EsQueryExec [index]
 *  </pre>
 *  Now even if there's a pushdown, the <i>final</i> plan would be:
 *  <pre>
 *  ProjectExec [_doc, foo, x]
 *  └── EsQueryExec [index with some TopN pushdown]
 *  </pre>
 *  The above actually reads the {@code x} field "unnecessarily", since it's only needed to conform to the output schema of the original
 *  plan. See #134363 for a way to optimize this little problem.
*/
public class AvoidFieldExtractionAfterTopN extends Rule<PhysicalPlan, PhysicalPlan> {
    @Override
    public PhysicalPlan apply(PhysicalPlan plan) {
        return dataDriverOutput(plan).<PhysicalPlan>map(attrs -> new ProjectExec(plan.source(), ((ProjectExec) plan).child(), attrs))
            .orElse(plan);
    }

    /**
     * Returns the expected output of the {@link TopNExec}, if the plan is a {@link ProjectExec} on top of a {@link TopNExec} and the
     * subplan is compatible with a data/reduce split, otherwise returns {@link Optional#empty()}.
     */
    public static Optional<List<Attribute>> dataDriverOutput(PhysicalPlan plan) {
        if (plan instanceof ProjectExec project && project.child() instanceof TopNExec topN && isTopNCompatible(topN)) {
            // We don't really care about fieldExtractPreference here, we just need to pass *something* for the output schema.
            return Optional.of(InsertFieldExtraction.rule(topN, MappedFieldType.FieldExtractPreference.NONE).output());
        }
        return Optional.empty();
    }

    /**
     * We don't support this optimization for multi-index queries at the moment, since the reduce coordinator doesn't actually have access
     * to each individual table's schema, and thus cannot determine the correct output of the data node's physical plan. Similarly, we don't
     * handle enrich or join yet.
     */
    private static boolean isTopNCompatible(PhysicalPlan topN) {
        return topN.anyMatch(plan -> plan instanceof EsQueryExec eqe && eqe.indexNameWithModes().size() > 1) == false
            && topN.anyMatch(plan -> plan instanceof EnrichExec) == false
            && topN.anyMatch(plan -> plan instanceof LookupJoinExec) == false
            && topN.anyMatch(plan -> plan instanceof HashJoinExec) == false;
    }
}
