/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
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

import java.util.LinkedList;
import java.util.List;

/**
 * Locate any TopN that is "visible" under remote ENRICH, and make a copy of it above the ENRICH,
 * while making a copy of the original fields. Mark the original TopN as local.
 * This is the same idea as {@link HoistRemoteEnrichLimit} but for TopN instead of Limit.
 * This must happen after {@link ReplaceLimitAndSortAsTopN}.
 */
public final class HoistRemoteEnrichTopN extends OptimizerRules.ParameterizedOptimizerRule<Enrich, LogicalOptimizerContext>
    implements
        OptimizerRules.CoordinatorOnly {
    public HoistRemoteEnrichTopN() {
        super(OptimizerRules.TransformDirection.UP);
    }

    @Override
    protected LogicalPlan rule(Enrich en, LogicalOptimizerContext ctx) {
        if (en.mode() == Enrich.Mode.REMOTE) {
            LogicalPlan plan = en.child();
            // This loop only takes care of one TopN, repeated application will stack them in correct order.
            while (true) {
                if (plan instanceof TopN top && top.isLocal() == false) {
                    // Create a fake OrderBy and "push" Enrich through it to generate aliases
                    Enrich topWithEnrich = (Enrich) en.replaceChild(new OrderBy(top.source(), en.child(), top.order()));
                    LogicalPlan pushPlan = PushDownUtils.pushGeneratingPlanPastProjectAndOrderBy(topWithEnrich);
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
                        LogicalPlan replacementTop = eval.replaceChild(top.withLocal(true));
                        LogicalPlan transformedEnrich = en.transformDown(p -> switch (p) {
                            case TopN t when t == top -> replacementTop;
                            // We only need to take care of Project because Drop can't drop our newly created fields
                            case Project pr -> {
                                List<NamedExpression> allFields = new LinkedList<>(pr.projections());
                                allFields.addAll(eval.fields());
                                yield pr.withProjections(allFields);
                            }
                            default -> p;
                        });

                        // Create the copied topN on top of the Enrich
                        var copyTop = new TopN(top.source(), transformedEnrich, order.order(), top.limit(), false);
                        // And use the project to remove the fields that we don't need anymore
                        return proj.replaceChild(copyTop);
                    } else {
                        // No need for aliasing - then it's simple, just copy the TopN on top and mark the original as local
                        LogicalPlan transformedEnrich = en.transformDown(TopN.class, t -> t == top ? top.withLocal(true) : t);
                        return new TopN(top.source(), transformedEnrich, top.order(), top.limit());
                    }
                }
                if ((plan instanceof CardinalityPreserving) == false // can change the number of rows, so we can't just pull a TopN from
                                                                     // under it
                    // this will fail the verifier anyway, so no need to continue
                    || (plan instanceof ExecutesOn ex && ex.executesOn() == ExecutesOn.ExecuteLocation.COORDINATOR)
                    // This is essentially another remote Enrich, it can handle its own limits
                    || (plan instanceof Enrich e && e.mode() == Enrich.Mode.REMOTE)
                    || plan instanceof PipelineBreaker) {
                    break;
                }
                if (plan instanceof UnaryPlan u) {
                    plan = u.child();
                } else {
                    // TODO: can we handle binary plans as well here?
                    break;
                }
            }
        }
        return en;
    }
}
