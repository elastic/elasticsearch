/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.Strings;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

/** Anchors sequence source names before field discovery so snapshot setup cannot resolve a different date-based index. */
final class SequenceSourceExpressions {
    private SequenceSourceExpressions() {}

    /** Preserves aliases and routing by resolving only date math, leaving ordinary sibling sources untouched. */
    static LogicalPlan resolve(LogicalPlan plan, long startedMillis) {
        return plan.transformUp(
            Sequence.class,
            sequence -> sequence.transformUp(
                UnresolvedRelation.class,
                relation -> relation.transformPropertiesOnly(IndexPattern.class, pattern -> resolve(pattern, startedMillis))
            )
        );
    }

    private static IndexPattern resolve(IndexPattern pattern, long startedMillis) {
        String[] expressions = Strings.commaDelimitedListToStringArray(pattern.indexPattern());
        boolean changed = false;
        for (int index = 0; index < expressions.length; index++) {
            var qualified = RemoteClusterAware.splitIndexName(expressions[index]);
            String expression = qualified.indexExpression();
            boolean excluded = expression.startsWith("-");
            String base = excluded ? expression.substring(1) : expression;
            var selector = IndexNameExpressionResolver.splitSelectorExpression(base);
            String resolved = IndexNameExpressionResolver.resolveDateMathExpression(selector.v1(), startedMillis);
            if (resolved.equals(selector.v1()) == false) {
                if (selector.v2() != null) {
                    resolved += "::" + selector.v2();
                }
                if (excluded) {
                    resolved = "-" + resolved;
                }
                expressions[index] = new RemoteClusterAware.QualifiedIndexExpression(qualified.clusterAlias(), resolved).toString();
                changed = true;
            }
        }
        return changed ? new IndexPattern(pattern.source(), String.join(",", expressions)) : pattern;
    }
}
