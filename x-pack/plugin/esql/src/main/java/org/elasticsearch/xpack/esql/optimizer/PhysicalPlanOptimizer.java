/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.common.Failure;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.esql.core.rule.Rule;
import org.elasticsearch.xpack.esql.core.rule.RuleExecutor;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.HashJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.MvExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RegexExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * This class is part of the planner. Performs global (coordinator) optimization of the physical plan. Local (data-node) optimizations
 * occur later by operating just on a plan {@link FragmentExec} (subplan).
 */
public class PhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, PhysicalOptimizerContext> {
    private static final Iterable<RuleExecutor.Batch<PhysicalPlan>> rules = initializeRules(true);

    private final PhysicalVerifier verifier = PhysicalVerifier.INSTANCE;

    public PhysicalPlanOptimizer(PhysicalOptimizerContext context) {
        super(context);
    }

    public PhysicalPlan optimize(PhysicalPlan plan) {
        return verify(execute(plan));
    }

    PhysicalPlan verify(PhysicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    static List<RuleExecutor.Batch<PhysicalPlan>> initializeRules(boolean isOptimizedForEsSource) {
        var boundary = new Batch<>("Plan Boundary", Limiter.ONCE, new ProjectAwayColumns());
        return asList(boundary);
    }

    @Override
    protected Iterable<RuleExecutor.Batch<PhysicalPlan>> batches() {
        return rules;
    }

    /**
     * Adds an explicit project to minimize the amount of attributes sent from the local plan to the coordinator.
     * This is done here to localize the project close to the data source and simplify the upcoming field
     * extraction.
     */
    static class ProjectAwayColumns extends Rule<PhysicalPlan, PhysicalPlan> {

        @Override
        public PhysicalPlan apply(PhysicalPlan plan) {
            var projectAll = new Holder<>(TRUE);
            var keepCollecting = new Holder<>(TRUE);
            var attributes = new AttributeSet();
            var aliases = new AttributeMap<Expression>();

            return plan.transformDown(UnaryExec.class, p -> {
                // no need for project all
                if (p instanceof ProjectExec || p instanceof AggregateExec) {
                    projectAll.set(FALSE);
                }
                if (keepCollecting.get()) {
                    p.forEachExpression(NamedExpression.class, ne -> {
                        var attr = ne.toAttribute();
                        // filter out attributes declared as aliases before
                        if (ne instanceof Alias as) {
                            aliases.put(attr, as.child());
                            attributes.remove(attr);
                        } else {
                            // skip synthetically added attributes (the ones from AVG), see LogicalPlanOptimizer.SubstituteSurrogates
                            if (attr.synthetic() == false && aliases.containsKey(attr) == false) {
                                attributes.add(attr);
                            }
                        }
                    });
                    if (p instanceof RegexExtractExec ree) {
                        attributes.removeAll(ree.extractedFields());
                    }
                    if (p instanceof MvExpandExec mvee) {
                        attributes.remove(mvee.expanded());
                    }
                    if (p instanceof HashJoinExec join) {
                        attributes.removeAll(join.addedFields());
                        for (Attribute rhs : join.rightFields()) {
                            if (join.leftFields().stream().anyMatch(x -> x.semanticEquals(rhs)) == false) {
                                attributes.remove(rhs);
                            }
                        }
                    }
                    if (p instanceof EnrichExec ee) {
                        for (NamedExpression enrichField : ee.enrichFields()) {
                            // TODO: why is this different then the remove above?
                            attributes.remove(enrichField instanceof Alias a ? a.child() : enrichField);
                        }
                    }
                }
                if (p instanceof ExchangeExec exec) {
                    keepCollecting.set(FALSE);
                    var child = exec.child();
                    // otherwise expect a Fragment
                    if (child instanceof FragmentExec fragmentExec) {
                        var logicalFragment = fragmentExec.fragment();

                        // no need for projection when dealing with aggs
                        if (logicalFragment instanceof Aggregate == false) {
                            var selectAll = projectAll.get();
                            var output = selectAll ? exec.child().output() : new ArrayList<>(attributes);
                            // if all the fields are filtered out, it's only the count that matters
                            // however until a proper fix (see https://github.com/elastic/elasticsearch/issues/98703)
                            // add a synthetic field (so it doesn't clash with the user defined one) to return a constant
                            // to avoid the block from being trimmed
                            if (output.isEmpty()) {
                                var alias = new Alias(logicalFragment.source(), "<all-fields-projected>", null, Literal.NULL, null, true);
                                List<Alias> fields = singletonList(alias);
                                logicalFragment = new Eval(logicalFragment.source(), logicalFragment, fields);
                                output = Expressions.asAttributes(fields);
                            }
                            // add a logical projection (let the local replanning remove it if needed)
                            p = exec.replaceChild(
                                new FragmentExec(
                                    Source.EMPTY,
                                    new Project(logicalFragment.source(), logicalFragment, output),
                                    fragmentExec.esFilter(),
                                    fragmentExec.estimatedRowSize(),
                                    fragmentExec.reducer()
                                )
                            );
                        }
                    }
                }
                return p;
            });
        }
    }
}
