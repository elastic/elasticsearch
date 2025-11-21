/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.optimizer.LocalPhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.InsertFieldExtraction;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.PushTopNToSource;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.ReplaceSourceAttributes;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EstimatesRowSize;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSinkExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeSourceExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
import org.elasticsearch.xpack.esql.planner.mapper.LocalMapper;
import org.elasticsearch.xpack.esql.stats.SearchStats;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

/**
* Modify a {@link Project} that follows a {@link TopN} such that it tries to minimize field extraction on the data driver.
*
* Consider the following query:
* <pre>
* FROM index | WHERE x > 10 | SORT foo | LIMIT 10 | KEEP bar
* </pre>
* If we can delay materializing {@code bar} until the node-reduce driver has finished its own TopN, we can reduce the amount of data we
* read from the index.
*
* The basic strategy here is to "cut off" the operation right after the last top n, and perform all the removed operations on the
* node reduce drivier, so the data drivers top n operations "feed into" the node reduce one. Ideally, we would just take the top-most
* {@link TopNExec}, but unfortunately that doesn't quite work: the top n might be pushed down to the source in {@link PushTopNToSource},
* which might change the output attributes (the filter might also be pushed down, so no {@code x} will be output). To solve this, we add a
* {@link Project} to ensure that the output schema of the data-side plan remains consistent with the expectations of the reduce-side
* plan (note that while performing the reduce-side plan we have no way of knowing if a pushdown is possible or not, since we don't have
* access to the source's capabilities).
*
* So for the aforementioned query, we would go from (roughly) this plan:
* <pre>
*  Project [bar]
*  └── TopN [foo, limit=10] (this will output _doc, foo, and x)
*      └── Filter [x > 10]
*          └── EsRelation [index]
*  </pre>
*  Into this:
*  <pre>
*  Project [_doc, foo, x]
*  └── TopN [foo, limit=10]
*      └── Filter [x > 10]
*          └── EsRelation [index]
*  </pre>
*  Now even if there's a pushdown, the <i>final</i> plan would be:
*  <pre>
*  Project [_doc, foo, x]
*  └── EsQuery [index with some TopN pushdown]
*  </pre>
*  The above actually reads the {@code x} field "unnecessarily", since it's only needed to conform to the output schema of the original
*  plan. See #134363 for a way to optimize this little problem.
*/
class LateMaterializationPlanner {
    public static Optional<ReductionPlan> planReduceDriverTopN(
        Function<SearchStats, LocalPhysicalOptimizerContext> contextFactory,
        ExchangeSinkExec originalPlan
    ) {
        FragmentExec fragmentExec = originalPlan.child() instanceof FragmentExec fe ? fe : null;
        if (fragmentExec == null) {
            return Optional.empty();
        }

        Project topLevelProject = fragmentExec.fragment() instanceof Project p ? p : null;
        if (topLevelProject == null) {
            return Optional.empty();
        }

        TopN topN = topLevelProject.child() instanceof TopN tn ? tn : null;
        if (topN == null) { // I'm getting go déjà vu
            return Optional.empty();
        }

        LocalPhysicalOptimizerContext context = contextFactory.apply(SEARCH_STATS_TOP_N_REPLACEMENT);
        List<Attribute> expectedDataOutput = toPhysical(topN, context).output();
        Attribute doc = expectedDataOutput.stream().filter(EsQueryExec::isDocAttribute).findFirst().orElse(null);
        if (doc == null) {
            return Optional.empty();
        }

        LogicalPlan withAddedDocToRelation = topN.transformUp(EsRelation.class, r -> {
            if (r.indexMode() == IndexMode.LOOKUP) {
                return r;
            }
            List<Attribute> attributes = CollectionUtils.prependToCopy(doc, r.output());
            return r.withAttributes(attributes);
        });
        if (withAddedDocToRelation.output().stream().noneMatch(EsQueryExec::isDocAttribute)) {
            // Defensive check: if any intermediate projects (or possibly another operator) removed the doc field, just abort this
            // optimization altogether!
            return Optional.empty();
        }

        // We need to add the doc attribute to the project since otherwise when the fragment is converted to a physical plan for the data
        // driver, the resulting ProjectExec won't have the doc attribute in its output, which is needed by the reduce driver.
        var updatedFragment = new Project(Source.EMPTY, withAddedDocToRelation, expectedDataOutput);
        FragmentExec updatedFragmentExec = fragmentExec.withFragment(updatedFragment);
        ExchangeSinkExec updatedDataPlan = originalPlan.replaceChild(updatedFragmentExec);

        // Replace the TopN child with the data driver as the source.
        PhysicalPlan reductionPlan = toPhysical(fragmentExec.fragment(), context).transformDown(
            TopNExec.class,
            t -> t.replaceChild(new ExchangeSourceExec(topN.source(), expectedDataOutput, false /* isIntermediateAgg */))
        );
        ExchangeSinkExec reductionPlanWithSize = originalPlan.replaceChild(
            EstimatesRowSize.estimateRowSize(updatedFragmentExec.estimatedRowSize(), reductionPlan)
        );

        return Optional.of(new ReductionPlan(reductionPlanWithSize, updatedDataPlan));
    }

    private static PhysicalPlan toPhysical(LogicalPlan plan, LocalPhysicalOptimizerContext context) {
        return new InsertFieldExtraction().apply(new ReplaceSourceAttributes().apply(new LocalMapper().map(plan)), context);
    }

    private LateMaterializationPlanner() { /* static class */ }

    // A hack to avoid the ReplaceFieldWithConstantOrNull optimization, since we don't have search stats during the reduce planning phase.
    // This sidesteps the issue by just assuming all fields exist and have no other meaningful stats. The local data optimizer will use the
    // real statistics.
    private static final SearchStats SEARCH_STATS_TOP_N_REPLACEMENT = new SearchStats.UnsupportedSearchStats() {
        @Override
        public boolean exists(FieldAttribute.FieldName field) {
            return true;
        }

        @Override
        public boolean isIndexed(FieldAttribute.FieldName field) {
            return false;
        }

        @Override
        public Object min(FieldAttribute.FieldName field) {
            return null;
        }

        @Override
        public Object max(FieldAttribute.FieldName field) {
            return null;
        }
    };
}
