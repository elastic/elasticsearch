/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
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
                new ResolveRefs(),
                new ResolveFunctions());
        
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

    private static class ResolveRefs extends AnalyzerRule<LogicalPlan> {

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

    private class ResolveFunctions extends AnalyzerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan.transformExpressionsUp(e -> {
                if (e instanceof UnresolvedFunction) {
                    UnresolvedFunction uf = (UnresolvedFunction) e;

                    if (uf.analyzed()) {
                        return uf;
                    }

                    String name = uf.name();

                    if (uf.childrenResolved() == false) {
                        return uf;
                    }

                    String functionName = functionRegistry.resolveAlias(name);
                    if (functionRegistry.functionExists(functionName) == false) {
                        return uf.missing(functionName, functionRegistry.listFunctions());
                    }
                    FunctionDefinition def = functionRegistry.resolveFunction(functionName);
                    Function f = uf.buildResolved(null, def);
                    return f;
                }
                return e;
            });
        }
    }
}