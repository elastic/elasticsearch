/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.Order;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.ExecutesOn;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.PipelineBreaker;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Streaming;
import org.elasticsearch.xpack.esql.plan.logical.TopN;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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
            LogicalPlan plan = en.child();
            List<Attribute> outputs = en.output();
            // This loop only takes care of one TopN. Repeated TopNs may be a problem, we can't handle them here.
            while (true) {
                if (plan instanceof TopN topN && topN.local() == false) {
                    Set<Attribute> missingAttributes = checkOrderInOutputs(topN.order(), outputs);

                    // Do we need to do any aliasing because some of the fields were overridden by Enrich or other generating plans?
                    if (missingAttributes.isEmpty()) {
                        // No need for aliasing - then it's simple, just copy the TopN on top of Enrich and mark the original as local
                        LogicalPlan transformedEnrich = en.transformDown(TopN.class, t -> t == topN ? topN.withLocal(true) : t);
                        return new TopN(topN.source(), transformedEnrich, topN.order(), topN.limit(), false);
                    } else {
                        Set<String> missingNames = new HashSet<>(Expressions.names(missingAttributes));
                        AttributeMap.Builder<Alias> aliasesForReplacedAttributesBuilder = AttributeMap.builder();
                        List<Expression> rewrittenExpressions = new ArrayList<>();

                        // Replace the missing attributes in order expressions with their aliases
                        for (Order expr : topN.order()) {
                            rewrittenExpressions.add(expr.transformUp(Attribute.class, attr -> {
                                if (missingNames.contains(attr.name())) {
                                    Alias renamedAttribute = aliasesForReplacedAttributesBuilder.computeIfAbsent(attr, a -> {
                                        String tempName = TemporaryNameGenerator.locallyUniqueTemporaryName(a.name());
                                        return new Alias(a.source(), tempName, a, null, true);
                                    });
                                    return renamedAttribute.toAttribute();
                                }

                                return attr;
                            }));
                        }
                        var replacedAttributes = aliasesForReplacedAttributesBuilder.build();
                        // We need to create a copy of attributes used by TopN, because Enrich or some command on the way overwrites them.
                        // Shadowed structure is going to look like this:
                        /*
                         Project[[_meta_field{f}#16, first_name{f}#11, gender{f}#12, hire_date{f}#17, job{f}#18, job.raw{f}#19,...]]
                         \_TopN[[Order[$$emp_no$temp_name$26{r$}#27,ASC,LAST]],10[INTEGER],false]
                          \_Enrich[REMOTE,languages_remote[KEYWORD],id{r}#4,{"match":{"indices":[],"match_field":"id",
                         "enrich_fields":["language_code","language_name"]}},{=languages_idx},[language_code{r}#24, language_name{r}#25]]
                            \_Eval[[emp_no{f}#10 + 1[INTEGER] AS emp_no#8]]
                             \_Eval[[emp_no{f}#10 AS $$emp_no$temp_name$26#27]]
                               \_TopN[[Order[emp_no{f}#10,ASC,LAST]],10[INTEGER],true]
                                \_Eval[[emp_no{f}#10 AS id#4]]
                                 \_EsRelation[test][_meta_field{f}#16, emp_no{f}#10, first_name{f}#11, ..]
                         */
                        // Put Eval which renames the affected fields above the original TopN, and mark that one as local
                        Eval replacementTop = new Eval(topN.source(), topN.withLocal(true), new ArrayList<>(replacedAttributes.values()));
                        Holder<Boolean> stop = new Holder<>(false);
                        LogicalPlan transformedEnrich = en.transformDown(p -> switch (p) {
                            case TopN t when stop.get() == false && t == topN -> {
                                stop.set(true);
                                yield replacementTop;
                            }
                            // We only need to take care of Project because Drop can't possibly drop our newly created fields
                            case Project pr when stop.get() == false -> {
                                List<NamedExpression> allFields = new LinkedList<>(pr.projections());
                                allFields.addAll(replacementTop.fields());
                                yield pr.withProjections(allFields);
                            }
                            default -> p;
                        });
                        @SuppressWarnings("unchecked")
                        List<Order> newOrder = (List<Order>) (List<?>) rewrittenExpressions;

                        // Verify that the outputs of transformed Enrich contain all the fields TopN needs
                        // This can happen if there's a node that removes fields like Project and we didn't catch it above
                        var missing = checkOrderInOutputs(newOrder, transformedEnrich.output());
                        if (missing.isEmpty() == false) {
                            throw new IllegalStateException(
                                "Attribute ["
                                    + Expressions.names(missing)
                                    + "] required by TopN but is not in the outputs of Enrich ["
                                    + Expressions.names(transformedEnrich.output())
                                    + "]"
                            );
                        }

                        // Create a copy of TopN with the rewritten attributes on top on Enrich
                        var copyTop = new TopN(topN.source(), transformedEnrich, newOrder, topN.limit(), false);
                        // And use the Project to remove the fields that we don't need anymore
                        return new Project(en.source(), copyTop, outputs);
                    }
                }
                if ((plan instanceof Streaming) == false // can change the number of rows, so we can't just pull a TopN from
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

    /**
     * Checks if all attributes used in the order expressions are present in outputs.
     * Returns the set of missing attributes.
     */
    private Set<Attribute> checkOrderInOutputs(List<Order> orderList, List<Attribute> outputs) {
        var outputsSet = outputs.stream().map(Attribute::id).collect(Collectors.toSet());
        Set<Attribute> missingAttributes = new HashSet<>();
        for (Order o : orderList) {
            o.child().forEachDown(Attribute.class, a -> {
                if (outputsSet.contains(a.id()) == false) {
                    missingAttributes.add(a);
                }
            });
        }
        return missingAttributes;
    }
}
