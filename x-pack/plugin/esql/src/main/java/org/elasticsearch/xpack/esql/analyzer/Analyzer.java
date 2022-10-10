/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analyzer;

import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules;
import org.elasticsearch.xpack.ql.analyzer.AnalyzerRules.AnalyzerRule;
import org.elasticsearch.xpack.ql.common.Failure;
import org.elasticsearch.xpack.ql.expression.Attribute;
import org.elasticsearch.xpack.ql.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.ql.index.IndexResolution;
import org.elasticsearch.xpack.ql.plan.TableIdentifier;
import org.elasticsearch.xpack.ql.plan.logical.EsRelation;
import org.elasticsearch.xpack.ql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.ql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.ql.rule.RuleExecutor;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Analyzer extends RuleExecutor<LogicalPlan> {
    private final IndexResolution indexResolution;
    private final Verifier verifier;

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
        Batch resolution = new Batch("Resolution", new ResolveTable(), new ResolveAttributes());
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

            return new EsRelation(plan.source(), indexResolution.get(), plan.frozen());
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
}
