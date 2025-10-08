/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.GeneratingPlan;
import org.elasticsearch.xpack.esql.plan.logical.CardinalityPreserving;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.OrderBy;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Locate any TopN that is "visible" under remote ENRICH, and make a copy of it above the ENRICH,
 * while making a copy of the original fields. Mark the original TopN as local.
 * This is the same idea as {@link HoistRemoteEnrichLimit} but for TopN instead of Limit.
 * This must happen after {@link ReplaceLimitAndSortAsTopN}.
 * This also has handling for the case where Enrich overrides the field(s) TopN is using. In this case,
 * we need to create aliases for the fields used by TopN, and then use those aliases in the copy TopN.
 * Then we need to add Project to remove the alias fields. Fortunately, PushDownUtils has the logic to do that.
 */
public final class HoistRemoteEnrichTopN extends OptimizerRules.OptimizerRule<Enrich> implements OptimizerRules.CoordinatorOnly {
    public HoistRemoteEnrichTopN() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Enrich en) {
        if (en.mode() == Enrich.Mode.REMOTE) {
            List<NamedExpression> generatedAttributes = new ArrayList<>(en.generatedAttributes());
            LogicalPlan plan = en.child();
            // This loop only takes care of one TopN. Repeated TopNs may be a problem, we can't handle them here.
            while (true) {
                if (plan instanceof GeneratingPlan<?> gen) {
                    generatedAttributes.addAll(gen.generatedAttributes());
                }
                var outputs = en.output();
                if (plan instanceof TopN topN && topN.local() == false) {
                    // Create a fake OrderBy and "push" Enrich through it to generate aliases
                    OrderBy fakeOrderBy = new OrderBy(topN.source(), topN.child(), topN.order());
                    Enrich enrichWithOrderBy = new Enrich(
                        en.source(),
                        fakeOrderBy,
                        en.mode(),
                        en.policyName(),
                        en.matchField(),
                        en.policy(),
                        en.concreteIndices(),
                        // We add here all the attributes generated on the way, so if one of them is needed for ordering,
                        // it will be aliased
                        generatedAttributes
                    );
                    LogicalPlan pushPlan = PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(enrichWithOrderBy);
                    // If we needed to alias any names, the result would look like this:
                    // Project[[host{f}#14, timestamp{f}#16, user{f}#15, ip{r}#19, os{r}#20]]
                    // \_OrderBy[[Order[timestamp{f}#16,ASC,LAST], Order[user{f}#15,ASC,LAST],
                    // Order[$$ip$temp_name$21{r$}#22,ASC,LAST]]]
                    // \_Enrich[REMOTE,hosts[KEYWORD],ip{r}#3,{"match":{"indices":[],"match_field":"ip",
                    // "enrich_fields":["ip","os"]}},{},[ip{r}#19,os{r}#20]]
                    // \_Eval[[ip{r}#3 AS $$ip$temp_name$21#22]]
                    if (pushPlan instanceof Project proj) {
                        // We needed renaming - deconstruct the plan from above and extract the relevant parts
                        if ((proj.child() instanceof OrderBy o && o.child() instanceof Enrich e && e.child() instanceof Eval) == false) {
                            throw new IllegalStateException("Unexpected pushed plan structure: " + pushPlan);
                        }
                        OrderBy order = (OrderBy) proj.child();
                        Enrich enrich = (Enrich) order.child();
                        Eval eval = (Eval) enrich.child();
                        // We insert the evals above the original TopN, so that the copy TopN works on the renamed fields
                        LogicalPlan replacementTop = eval.replaceChild(topN.withLocal(true));
                        LogicalPlan transformedEnrich = en.transformDown(p -> switch (p) {
                            case TopN t when t == topN -> replacementTop;
                            // We only need to take care of Project because Drop can't possibly drop our newly created fields
                            case Project pr -> {
                                List<NamedExpression> allFields = new LinkedList<>(pr.projections());
                                allFields.addAll(eval.fields());
                                yield pr.withProjections(allFields);
                            }
                            default -> p;
                        });

                        // Create the copied topN on top of the Enrich
                        var copyTop = new TopN(topN.source(), transformedEnrich, order.order(), topN.limit(), false);
                        // And use the Project to remove the fields that we don't need anymore
                        return new Project(en.source(), copyTop, en.output());
                    } else {
                        // No need for aliasing - then it's simple, just copy the TopN on top of Enrich and mark the original as local
                        LogicalPlan transformedEnrich = en.transformDown(TopN.class, t -> t == topN ? topN.withLocal(true) : t);
                        return new TopN(topN.source(), transformedEnrich, topN.order(), topN.limit(), false);
                    }
                }
                if ((plan instanceof CardinalityPreserving) == false // can change the number of rows, so we can't just pull a TopN from
                                                                     // under it
                    // this will fail the verifier anyway, so no need to continue
                    || (plan instanceof ExecutesOn ex && ex.executesOn() == ExecutesOn.ExecuteLocation.COORDINATOR)
                    // This is another remote Enrich, it can handle its own limits
                    || (plan instanceof Enrich e && e.mode() == Enrich.Mode.REMOTE)
                    || plan instanceof PipelineBreaker) {
                    break;
                }
                if (plan instanceof UnaryPlan u) {
                    plan = u.child();
                } else {
                    // The only non-unary plans right now are Join and Fork, and they are not cardinality preserving,
                    // so really there's nothing to do here. But if we had binary plan that is cardinality preserving,
                    // we would need to add it here.
                    break;
                }
            }
        }
        return en;
    }
}
