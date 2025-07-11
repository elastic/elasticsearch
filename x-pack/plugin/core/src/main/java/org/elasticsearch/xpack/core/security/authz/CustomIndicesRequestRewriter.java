/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.RewritableIndicesRequest;

public interface CustomIndicesRequestRewriter {
    void rewrite(RewritableIndicesRequest request);

    class Default implements CustomIndicesRequestRewriter {
        @Override
        public void rewrite(RewritableIndicesRequest request) {
            // No rewriting by default
            // This is a no-op implementation
        }
    }
}
