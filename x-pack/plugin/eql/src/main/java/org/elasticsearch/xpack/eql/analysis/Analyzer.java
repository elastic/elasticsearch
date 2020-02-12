/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.eql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.Rule;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.eql.analysis.AnalysisUtils.resolveAgainstList;

public class Analyzer extends RuleExecutor<LogicalPlan> {

    private final FunctionRegistry functionRegistry;
    private final Verifier verifier;

    public Analyzer(FunctionRegistry functionRegistry, Verifier verifier) {
        this.functionRegistry = functionRegistry;
        this.verifier = verifier;
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch resolution = new Batch("Resolution",
                new ResolveRefs());
        
        return asList(resolution);
    }

    public LogicalPlan analyze(LogicalPlan plan) {
        return verify(execute(plan));
    }

    private LogicalPlan verify(LogicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (!failures.isEmpty()) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    private static class ResolveRefs extends AnalyzeRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            // if the children are not resolved, there's no way the node can be resolved
            if (!plan.childrenResolved()) {
                return plan;
            }

            // okay, there's a chance so let's get started
            if (log.isTraceEnabled()) {
                log.trace("Attempting to resolve {}", plan.nodeString());
            }

            return plan.transformExpressionsUp(e -> {
                if (e instanceof UnresolvedAttribute) {
                    UnresolvedAttribute u = (UnresolvedAttribute) e;
                    List<Attribute> childrenOutput = new ArrayList<>();
                    for (LogicalPlan child : plan.children()) {
                        childrenOutput.addAll(child.output());
                    }
                    NamedExpression named = resolveAgainstList(u, childrenOutput);
                    // if resolved, return it; otherwise keep it in place to be resolved later
                    if (named != null) {
                        if (log.isTraceEnabled()) {
                            log.trace("Resolved {} to {}", u, named);
                        }
                        return named;
                    }
                }
                return e;
            });
        }
    }

    abstract static class AnalyzeRule<SubPlan extends LogicalPlan> extends Rule<SubPlan, LogicalPlan> {

        // transformUp (post-order) - that is first children and then the node
        // but with a twist; only if the tree is not resolved or analyzed
        @Override
        public final LogicalPlan apply(LogicalPlan plan) {
            return plan.transformUp(t -> t.analyzed() || skipResolved() && t.resolved() ? t : rule(t), typeToken());
        }

        @Override
        protected abstract LogicalPlan rule(SubPlan plan);

        protected boolean skipResolved() {
            return true;
        }
    }
}