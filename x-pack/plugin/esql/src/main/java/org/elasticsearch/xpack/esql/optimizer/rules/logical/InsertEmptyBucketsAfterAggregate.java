/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.analysis.AnalyzerRules;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.expression.function.grouping.Bucket;
import org.elasticsearch.xpack.esql.expression.function.grouping.TBucket;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.InsertEmptyBuckets;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SequencedMap;

/**
 * Wraps an {@link Aggregate} whose groupings include one or more {@link Bucket}s (or {@link TBucket}s) with the option
 * {@code {"include_empty_buckets": true}} in a coordinator-only {@link InsertEmptyBuckets} node. This new node generates
 * the empty buckets. This must run before {@link TranslateTimeSeriesAggregate}, which rips the aggregate and buckets
 * apart.
 */
public class InsertEmptyBucketsAfterAggregate extends AnalyzerRules.AnalyzerRule<LogicalPlan> {

    @Override
    protected boolean skipResolved() {
        // The aggregates are fully resolved by the time we want to wrap them.
        return false;
    }

    /**
     * This wraps the children of {@code plan}, so that the parent context is here.
     * Children of {@link InlineStats} must not be wrapped.
     */
    @Override
    protected LogicalPlan rule(LogicalPlan plan) {
        if (plan instanceof InlineStats || plan instanceof InsertEmptyBuckets) {
            return plan;
        }
        List<LogicalPlan> newChildren = plan.children()
            .stream()
            .map(child -> child instanceof Aggregate aggregate ? maybeWrap(aggregate) : child)
            .toList();
        return newChildren.equals(plan.children()) ? plan : plan.replaceChildren(newChildren);
    }

    private static LogicalPlan maybeWrap(Aggregate aggregate) {
        if (aggregate.resolved() == false) {
            // This runs during analysis (before verification) with skipResolved()==false, so it also sees not-yet-resolved plans. Reading
            // the options off a BUCKET/TBUCKET grouping via includeEmptyBuckets() throws on invalid options; skip unresolved aggregates so
            // the Verifier can instead surface a clean VerificationException.
            return aggregate;
        }
        SequencedMap<Attribute, Bucket> buckets = new LinkedHashMap<>();
        List<Attribute> groups = new ArrayList<>();
        for (var grouping : aggregate.groupings()) {
            Attribute attribute = Expressions.attribute(grouping);
            Expression g = Alias.unwrap(grouping);
            if (g instanceof Bucket bucket && bucket.includeEmptyBuckets()) {
                buckets.put(attribute, bucket);
            } else if (g instanceof TBucket tbucket && tbucket.includeEmptyBuckets()) {
                // TBUCKET is only rewritten to a BUCKET later, by SubstituteSurrogateExpressions; detect it here, while it is
                // still inline in the grouping, and store its surrogate BUCKET (which carries the include_empty_buckets option).
                buckets.put(attribute, (Bucket) tbucket.surrogate());
            } else {
                groups.add(attribute);
            }
        }
        if (buckets.isEmpty()) {
            return aggregate;
        }
        // Set default values to null here; PushdownInsertEmptyBucketsAndSetDefaultValues
        // will determine them later, after ReplaceAggregateAggExpressionWithEval has run.
        return new InsertEmptyBuckets(aggregate.source(), aggregate, buckets, groups, null);
    }
}
