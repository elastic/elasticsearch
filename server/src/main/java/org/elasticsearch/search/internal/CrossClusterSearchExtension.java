/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.action.IndicesRequest;

public interface CrossClusterSearchExtension {

    IndicesExpressionRewriter indicesExpressionRewriter();

    interface IndicesExpressionRewriter {
        IndicesRequest.Replaceable rewrite(IndicesRequest.Replaceable request);
    }

    class Default implements CrossClusterSearchExtension {
        public Default() {}

        @Override
        public IndicesExpressionRewriter indicesExpressionRewriter() {
            return request -> {
                // Default implementation does nothing, can be overridden by extensions
                return request;
            };
        }
    }
}
