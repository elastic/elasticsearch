/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.xpack.esql.plan.physical.AggregateExec;
import org.elasticsearch.xpack.esql.plan.physical.EnrichExec;
import org.elasticsearch.xpack.esql.plan.physical.ExchangeExec;
import org.elasticsearch.xpack.esql.plan.physical.FragmentExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.plan.physical.RegexExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.UnaryExec;
import org.elasticsearch.xpack.esql.planner.PhysicalVerificationException;
import org.elasticsearch.xpack.esql.planner.PhysicalVerifier;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Alias;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.FieldAttribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.plan.logical.Aggregate;
import org.elasticsearch.xpack.ql.plan.logical.Project;
import org.elasticsearch.xpack.ql.rule.ParameterizedRuleExecutor;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.Holder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;

/**
 * Performs global (coordinator) optimization of the physical plan.
 * Local (data-node) optimizations occur later by operating just on a plan fragment (subplan).
 */
public class PhysicalPlanOptimizer extends ParameterizedRuleExecutor<PhysicalPlan, PhysicalOptimizerContext> {
    private static final Iterable<RuleExecutor.Batch<PhysicalPlan>> rules = initializeRules(true);

    private final PhysicalVerifier verifier;

    public PhysicalPlanOptimizer(PhysicalOptimizerContext context) {
        super(context);
        this.verifier = new PhysicalVerifier();
    }

    public PhysicalPlan optimize(PhysicalPlan plan) {
        return verify(execute(plan));
    }

    PhysicalPlan verify(PhysicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (failures.isEmpty() == false) {
            throw new PhysicalVerificationException(failures);
        }
        return plan;
    }

    static List<RuleExecutor.Batch<PhysicalPlan>> initializeRules(boolean isOptimizedForEsSource) {
        var boundary = new Batch<PhysicalPlan>("Plan Boundary", Limiter.ONCE, new ProjectAwayColumns());
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
            var attributes = new LinkedHashSet<Attribute>();
            var aliases = new HashMap<Attribute, Expression>();
            var fields = new LinkedHashSet<FieldAttribute>();

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
                            if (aliases.containsKey(attr) == false) {
                                attributes.add(attr);
                                // track required (materialized) fields
                                if (ne instanceof FieldAttribute fa) {
                                    fields.add(fa);
                                }
                            }
                        }
                    });
                    if (p instanceof RegexExtractExec ree) {
                        attributes.removeAll(ree.extractedFields());
                    }
                    if (p instanceof EnrichExec ee) {
                        for (NamedExpression enrichField : ee.enrichFields()) {
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
                        if (logicalFragment instanceof Aggregate) {
                            attributes.clear();
                        }
                        var selectAll = projectAll.get();
                        if (attributes.isEmpty() == false || selectAll) {
                            var output = selectAll ? exec.child().output() : new ArrayList<>(attributes);
                            // add a logical projection (let the local replanning remove it if needed)
                            p = exec.replaceChild(
                                new FragmentExec(
                                    Source.EMPTY,
                                    new Project(logicalFragment.source(), logicalFragment, output),
                                    fragmentExec.esFilter()
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
