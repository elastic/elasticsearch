/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An interface for requests that have had their index expressions replaced/resolved.
 * TODO: does this actually make sense as an interface?
 */
public interface ReplacedIndexExpressions {
    String[] indices();

    default List<String> indicesAsList() {
        return List.of(indices());
    }

    @Nullable
    default Map<String, ReplacedIndexExpression> asMap() {
        return null;
    }

    record CompleteReplacedIndexExpressions(Map<String, ReplacedIndexExpression> replacedExpressionMap)
        implements
            ReplacedIndexExpressions {
        @Override
        public String[] indices() {
            return ReplacedIndexExpression.toIndices(replacedExpressionMap);
        }

        @Override
        public Map<String, ReplacedIndexExpression> asMap() {
            return replacedExpressionMap;
        }
    }

    record CrossProjectReplacedIndexExpressions(Map<String, ReplacedIndexExpression> replacedExpressionMap)
        implements
            ReplacedIndexExpressions {

        public static boolean isQualifiedIndexExpression(String indexExpression) {
            return RemoteClusterAware.isRemoteIndexName(indexExpression);
        }

        @Override
        public String[] indices() {
            return ReplacedIndexExpression.toIndices(replacedExpressionMap);
        }

        @Override
        public Map<String, ReplacedIndexExpression> asMap() {
            return replacedExpressionMap;
        }

        public List<String> getLocalExpressions() {
            return replacedExpressionMap.values()
                .stream()
                .filter(e -> hasCanonicalExpressionForOrigin(e.replacedBy()))
                .map(ReplacedIndexExpression::original)
                .toList();
        }

        public static boolean hasCanonicalExpressionForOrigin(List<String> replacedBy) {
            return replacedBy.stream().anyMatch(e -> false == isQualifiedIndexExpression(e));
        }

        public void replaceLocalExpressions(ReplacedIndexExpressions.CompleteReplacedIndexExpressions localResolved) {
            if (localResolved == null || localResolved.asMap() == null || localResolved.asMap().isEmpty()) {
                return;
            }

            for (Map.Entry<String, ReplacedIndexExpression> e : localResolved.asMap().entrySet()) {
                final String original = e.getKey();
                final ReplacedIndexExpression local = e.getValue();
                if (local == null) {
                    continue;
                }

                final ReplacedIndexExpression current = replacedExpressionMap.get(original);
                if (current == null) {
                    continue;
                }

                final List<String> qualified = current.replacedBy()
                    .stream()
                    .filter(CrossProjectReplacedIndexExpressions::isQualifiedIndexExpression)
                    .toList();

                final List<String> resolvedLocal = local.replacedBy();

                final List<String> combined = new ArrayList<>(resolvedLocal.size() + qualified.size());
                combined.addAll(resolvedLocal);
                combined.addAll(qualified);

                replacedExpressionMap.put(
                    original,
                    new ReplacedIndexExpression(original, combined, local.authorized(), local.existsAndVisible(), null)
                );
            }
        }
    }
}
