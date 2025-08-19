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
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.List;

// Extend indices.replaceable instead?
public interface CrossProjectResolvableRequest extends IndicesRequest {
    /**
     * Only called if cross-project rewriting was applied
     */
    void setRewrittenExpressions(List<RewrittenExpression> rewrittenExpressions);

    @Nullable
    List<RewrittenExpression> getRewrittenExpressions();

    default boolean isCrossProjectModeEnabled() {
        return getRewrittenExpressions() != null;
    }

    /**
     * Used to track a mapping from original expression (potentially flat) to canonical CCS expressions.
     */
    record RewrittenExpression(String original, List<CanonicalExpression> canonicalExpressions) {}

    record CanonicalExpression(String expression) {
        public boolean isQualified() {
            return RemoteClusterAware.isRemoteIndexName(expression);
        }

        public boolean isUnqualified() {
            return false == isQualified();
        }
    }
}
