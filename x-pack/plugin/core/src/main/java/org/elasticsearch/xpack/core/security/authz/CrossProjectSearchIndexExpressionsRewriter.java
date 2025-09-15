/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;

public interface CrossProjectSearchIndexExpressionsRewriter {
    void rewriteIndexExpressions(IndicesRequest.Replaceable request, ActionListener<Void> listener);

    class Default implements CrossProjectSearchIndexExpressionsRewriter {
        @Override
        public void rewriteIndexExpressions(IndicesRequest.Replaceable request, ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    }
}
