/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.analysis;

import org.elasticsearch.xpack.eql.expression.OptionalUnresolvedAttribute;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.AddMissingEqualsToBoolField;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.Expression;
import org.elasticsearch.xpack.ql.expression.Literal;
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
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.elasticsearch.xpack.eql.analysis.AnalysisUtils.resolveAgainstList;

public class Analyzer extends RuleExecutor<LogicalPlan> {

    private final Configuration configuration;
    private final FunctionRegistry functionRegistry;
    private final Verifier verifier;
    private Set<Expression> keyOptionals = emptySet(); // join keys optional fields

    public Analyzer(Configuration configuration, FunctionRegistry functionRegistry, Verifier verifier) {
        this.configuration = configuration;
        this.functionRegistry = functionRegistry;
        this.verifier = verifier;
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch resolution = new Batch("Resolution",
                new ResolveRefs(),
                new ResolveFunctions());

        Batch cleanup = new Batch("Finish Analysis", Limiter.ONCE,
                new AddMissingEqualsToBoolField());

        return asList(resolution, cleanup);
    }

    public LogicalPlan analyze(LogicalPlan plan) {
        return verify(execute(plan));
    }

    public void keyOptionals(Set<Expression> keyOptionals) {
        this.keyOptionals = keyOptionals;
    }

    private LogicalPlan verify(LogicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan, configuration.versionIncompatibleClusters(), keyOptionals);
        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    private class ResolveRefs extends AnalyzerRule<LogicalPlan> {

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
                Expression named = resolveAgainstList(u, childrenOutput);
                if (named == null) {
                    // if the attribute is not resolved (it doesn't exist in mappings) and it's an optional field, replace it with "null"
                    // in queries only (the Filter plan denotes the unresolved attribute lives in a query vs in a join key)
                    if (u instanceof OptionalUnresolvedAttribute) {
                        if (plan instanceof Filter) {
                            named = new Literal(u.source(), null, DataTypes.NULL);
                        } else if (u.resolved() == false && keyOptionals.contains(u)) {
                            if (log.isTraceEnabled()) {
                                log.trace("Resolved optional field {}", u);
                            }
                            ((OptionalUnresolvedAttribute) u).markAsResolved();
                        }
                    }
                } else {
                    // if the attribute is resolved and it is an optional field, update the list of optionals with its resolved variant
                    if (keyOptionals.contains(u)) {
                        keyOptionals.remove(u);
                        keyOptionals.add(named);
                    }
                }

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
}
