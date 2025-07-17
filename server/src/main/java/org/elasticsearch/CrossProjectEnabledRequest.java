/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch;

import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.core.Nullable;

import java.util.List;

public interface CrossProjectEnabledRequest extends IndicesRequest {
    /**
     * Can be used to determine if this should be processed in cross-project mode vs. stateful CCS.
     */
    boolean crossProjectModeEnabled();

    /**
     * Only called if cross-project rewriting (flat-world, linked project filtering) was applied
     */
    void qualified(List<QualifiedExpression> qualifiedExpressions);

    @Nullable
    String queryRouting();

    /**
     * Used to track a mapping from original expression (potentially flat-world) to canonicalized CCS expressions.
     * e.g. for an original index expression `logs-*`, this would be:
     * original=logs-*
     * qualified=[(logs-*, _local), (my-remote:logs-*, my-remote)]
     */
    record QualifiedExpression(String original, List<ExpressionWithProject> qualified) {
        public boolean hasFlatOriginalExpression() {
            return true;
        }
    }

    record ExpressionWithProject(String expression, String project) {}
}
