/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analyzer;

import org.elasticsearch.xpack.esql.plan.logical.EsQuery;
import org.elasticsearch.xpack.esql.plan.logical.FieldExtract;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.AnalyzerRule;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.expression.function.Function;
import org.elasticsearch.xpack.ql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.ql.expression.function.FunctionRegistry;
import org.elasticsearch.xpack.ql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;
import org.elasticsearch.xpack.ql.session.Configuration;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Analyzer extends RuleExecutor<LogicalPlan> {
    private final IndexResolution indexResolution;
    private final Verifier verifier;

    private final FunctionRegistry functionRegistry = new FunctionRegistry(FunctionRegistry.def(Avg.class, Avg::new, "AVG"));
    public static final ZoneId UTC = ZoneId.of("Z");
    public static final Configuration configuration = new Configuration(UTC, null, null, x -> Collections.emptySet());

    public Analyzer(IndexResolution indexResolution) {
        assert indexResolution != null;
        this.indexResolution = indexResolution;
        this.verifier = new Verifier();
    }

    public LogicalPlan analyze(LogicalPlan plan) {
        return verify(execute(plan));
    }

    public LogicalPlan verify(LogicalPlan plan) {
        Collection<Failure> failures = verifier.verify(plan);
        if (failures.isEmpty() == false) {
            throw new VerificationException(failures);
        }
        return plan;
    }

    @Override
    protected Iterable<RuleExecutor<LogicalPlan>.Batch> batches() {
        Batch resolution = new Batch("Resolution", new ResolveTable(), new ResolveAttributes(), new ResolveFunctions());
        return List.of(resolution);
    }

    private class ResolveTable extends AnalyzerRule<UnresolvedRelation> {
        @Override
        protected LogicalPlan rule(UnresolvedRelation plan) {
            if (indexResolution.isValid() == false) {
                return plan.unresolvedMessage().equals(indexResolution.toString())
                    ? plan
                    : new UnresolvedRelation(plan.source(), plan.table(), plan.alias(), plan.frozen(), indexResolution.toString());
            }
            TableIdentifier table = plan.table();
            if (indexResolution.matches(table.index()) == false) {
                new UnresolvedRelation(
                    plan.source(),
                    plan.table(),
                    plan.alias(),
                    plan.frozen(),
                    "invalid [" + table + "] resolution to [" + indexResolution + "]"
                );
            }

            EsQuery query = new EsQuery(plan.source(), indexResolution.get());
            return new FieldExtract(plan.source(), query, indexResolution.get(), query.output());
        }
    }

    public class ResolveAttributes extends AnalyzerRules.BaseAnalyzerRule {

        @Override
        protected LogicalPlan doRule(LogicalPlan plan) {
            Map<String, Attribute> scope = new HashMap<>();
            for (LogicalPlan child : plan.children()) {
                for (Attribute a : child.output()) {
                    scope.put(a.name(), a);
                }
            }

            return plan.transformExpressionsUp(UnresolvedAttribute.class, ua -> {
                Attribute resolved = scope.get(ua.qualifiedName());
                if (resolved != null) {
                    return resolved;
                } else {
                    return ua;
                }
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
