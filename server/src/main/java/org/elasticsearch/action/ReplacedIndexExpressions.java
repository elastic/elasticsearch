/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public record ReplacedIndexExpressions(Map<String, ReplacedIndexExpression> replacedExpressionMap) {
    public String[] getAllIndicesArray() {
        return replacedExpressionMap.values()
            .stream()
            .flatMap(indexExpression -> indexExpression.replacedBy().stream())
            .toArray(String[]::new);
    }

    public List<String> getLocalIndices() {
        return replacedExpressionMap.values().stream().flatMap(e -> e.getLocalIndices().stream()).toList();
    }

    public void replaceLocalIndices(ReplacedIndexExpressions localResolved) {
        if (localResolved == null || localResolved.replacedExpressionMap() == null || localResolved.replacedExpressionMap().isEmpty()) {
            return;
        }

        for (Map.Entry<String, ReplacedIndexExpression> e : localResolved.replacedExpressionMap().entrySet()) {
            final String original = e.getKey();
            final ReplacedIndexExpression local = e.getValue();
            if (local == null) {
                continue;
            }

            final ReplacedIndexExpression current = replacedExpressionMap.get(original);
            if (current == null) {
                // TODO this is an error, probably
                continue;
            }

            final List<String> remoteIndices = current.getRemoteIndices();

            final List<String> resolvedLocal = local.replacedBy();

            final List<String> combined = new ArrayList<>(resolvedLocal.size() + remoteIndices.size());
            combined.addAll(resolvedLocal);
            combined.addAll(remoteIndices);

            replacedExpressionMap.put(original, new ReplacedIndexExpression(original, combined, local.resolutionResult(), null));
        }
    }
}
