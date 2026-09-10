/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.NamedSubquery;
import org.elasticsearch.xpack.esql.plan.logical.SourceFanInUnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedExternalRelation;
import org.elasticsearch.xpack.esql.plan.logical.ViewUnionAll;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Lowers provisional source-expansion groups after authorized dataset rewriting and before
 * {@code PreAnalyzer}. A group with an actual external producer becomes the final flat
 * {@link SourceFanInUnionAll}. An index-only group is restored to the equivalent
 * {@link ViewUnionAll} (or the surviving producer) so ordinary index queries do not enter
 * the source-fan-in execution path.
 */
public final class SourceExpansionNormalizer {

    private SourceExpansionNormalizer() {}

    /**
     * Normalize every provisional source group in {@code plan}. Plans with no provisional
     * markers are returned unchanged.
     */
    public static LogicalPlan normalize(LogicalPlan plan) {
        if (plan.anyMatch(p -> p instanceof SourceFanInUnionAll fanIn && fanIn.isProvisional()) == false) {
            return plan;
        }
        return normalizeNode(plan);
    }

    private static LogicalPlan normalizeNode(LogicalPlan plan) {
        if (plan instanceof SourceFanInUnionAll fanIn && fanIn.isProvisional()) {
            return normalizeCandidate(fanIn);
        }
        List<LogicalPlan> children = plan.children();
        List<LogicalPlan> newChildren = null;
        for (int i = 0; i < children.size(); i++) {
            LogicalPlan next = normalizeNode(children.get(i));
            if (next != children.get(i)) {
                if (newChildren == null) {
                    newChildren = new ArrayList<>(children);
                }
                newChildren.set(i, next);
            }
        }
        return newChildren == null ? plan : plan.replaceChildren(newChildren);
    }

    /**
     * Decide this group's fate before lowering nested candidates. A bottom-up pass would
     * restore an index-only child to {@link ViewUnionAll} and then be tempted to promote
     * that wrapper when an outer group later found a dataset.
     */
    private static LogicalPlan normalizeCandidate(SourceFanInUnionAll candidate) {
        if (hasExternalProducer(candidate)) {
            List<LogicalPlan> leaves = new ArrayList<>();
            collectSourceLeaves(candidate, leaves);
            int definite = DatasetRewriter.definiteProducerCount(leaves);
            if (SourceFanInUnionAll.exceedsMaxProducers(definite)) {
                throw new VerificationException(
                    "FROM ["
                        + candidate.sourceText()
                        + "] resolved through view expansion to "
                        + definite
                        + " sources, exceeding the current limit of "
                        + SourceFanInUnionAll.MAX_PRODUCERS
                        + " per FROM. Narrow the pattern, exclude some datasets, or split into multiple queries."
                );
            }
            if (leaves.size() == 1) {
                return leaves.getFirst();
            }
            return new SourceFanInUnionAll(candidate.source(), leaves, candidate.output());
        }
        LinkedHashMap<String, LogicalPlan> restored = new LinkedHashMap<>();
        List<String> keys = candidate.branchKeys();
        List<LogicalPlan> children = candidate.children();
        for (int i = 0; i < children.size(); i++) {
            restored.put(keys.get(i), normalizeNode(children.get(i)));
        }
        if (restored.size() == 1) {
            return restored.values().iterator().next();
        }
        return new ViewUnionAll(candidate.source(), restored, candidate.output());
    }

    private static boolean hasExternalProducer(LogicalPlan plan) {
        LogicalPlan body = unwrapNamedSubquery(plan);
        if (body instanceof UnresolvedExternalRelation) {
            return true;
        }
        if (body instanceof SourceFanInUnionAll fanIn) {
            for (LogicalPlan child : fanIn.children()) {
                if (hasExternalProducer(child)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Collect eligible source leaves in order, flattening nested source candidates and
     * dropping only their structural wrappers. Speculative shadows stay as leaves.
     */
    private static void collectSourceLeaves(LogicalPlan plan, List<LogicalPlan> leaves) {
        LogicalPlan body = unwrapNamedSubquery(plan);
        if (body instanceof SourceFanInUnionAll nested) {
            for (LogicalPlan child : nested.children()) {
                collectSourceLeaves(child, leaves);
            }
            return;
        }
        leaves.add(body);
    }

    private static LogicalPlan unwrapNamedSubquery(LogicalPlan plan) {
        return plan instanceof NamedSubquery named ? named.child() : plan;
    }
}
