/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.eql.expression.OptionalMissingAttribute;
import org.elasticsearch.xpack.eql.expression.OptionalUnresolvedAttribute;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
import org.elasticsearch.xpack.ql.expression.NamedExpression;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.plan.logical.Filter;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.session.Configuration;
import org.elasticsearch.xpack.ql.type.DataTypes;

import java.util.Collection;
import java.util.LinkedHashSet;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.eql.analysis.AnalysisUtils.resolveAgainstList;
import static org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.AddMissingEqualsToBoolField;

public class Analyzer extends RuleExecutor<LogicalPlan> {

    private final Configuration configuration;
    private final FunctionRegistry functionRegistry;
    private final Verifier verifier;

    public Analyzer(Configuration configuration, FunctionRegistry functionRegistry, Verifier verifier) {
        this.configuration = configuration;
        this.functionRegistry = functionRegistry;
        this.verifier = verifier;
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch optional = new Batch("Optional", Limiter.ONCE, new ResolveOrReplaceOptionalRefs());

        Batch resolution = new Batch("Resolution", new ResolveRefs(), new ResolveFunctions());

        Batch cleanup = new Batch("Finish Analysis", Limiter.ONCE, new AddMissingEqualsToBoolField());

        return asList(optional, resolution, cleanup);
    }

    public LogicalPlan analyze(LogicalPlan plan) {
        return verify(execute(plan));
    }

    private LogicalPlan verify(LogicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan, configuration.versionIncompatibleClusters());
        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    private static class ResolveRefs extends AnalyzerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            // if the children are not resolved, there's no way the node can be resolved
            if (plan.childrenResolved() == false) {
                return plan;
            }

            // okay, there's a chance so let's get started
            if (log.isTraceEnabled()) {
                log.trace("Attempting to resolve {}", plan.nodeString());
            }

            return plan.transformExpressionsUp(UnresolvedAttribute.class, u -> {
                Collection<Attribute> childrenOutput = new LinkedHashSet<>();
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
                return u;
            });
        }
    }

    private class ResolveFunctions extends AnalyzerRule<LogicalPlan> {

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {
            return plan.transformExpressionsUp(UnresolvedFunction.class, uf -> {
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
                Function f = uf.buildResolved(configuration, def);
                return f;
            });
        }
    }

    private static class ResolveOrReplaceOptionalRefs extends AnalyzerRule<LogicalPlan> {

        @Override
        protected boolean skipResolved() {
            return false;
        }

        @Override
        protected LogicalPlan rule(LogicalPlan plan) {

            return plan.transformExpressionsUp(OptionalUnresolvedAttribute.class, u -> {
                Collection<Attribute> resolvedChildrenOutput = new LinkedHashSet<>();
                for (LogicalPlan child : plan.children()) {
                    for (Attribute out : child.output()) {
                        if (out.resolved()) {
                            resolvedChildrenOutput.addAll(child.output());
                        }
                    }
                }
                Expression resolved = resolveAgainstList(u, resolvedChildrenOutput);
                // if resolved, return it; otherwise replace it with the missing attribute
                if (resolved != null) {
                    if (log.isTraceEnabled()) {
                        log.trace("Resolved {} to {}", u, resolved);
                    }
                } else {
                    // when used in a filter, replace the field with a literal
                    if (plan instanceof Filter) {
                        resolved = new Literal(u.source(), null, DataTypes.NULL);
                    } else {
                        resolved = new OptionalMissingAttribute(u.source(), u.name(), u.qualifier());
                    }
                }
                return resolved;
            });

        }
    }
}
