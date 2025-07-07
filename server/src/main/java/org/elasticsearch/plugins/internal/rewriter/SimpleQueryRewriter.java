/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins.internal.rewriter;

import org.elasticsearch.index.query.QueryBuilder;

import java.util.Map;

public interface SimpleQueryRewriter {

    String getName();

    QueryBuilder rewrite(QueryBuilder queryBuilder);

    static SimpleQueryRewriter multi(Map<String, SimpleQueryRewriter> rewriters) {
        return rewriters.isEmpty() ? new NoOpSimpleQueryRewriter() : new CompositeSimpleQueryRewriter(rewriters);
    }

    class CompositeSimpleQueryRewriter implements SimpleQueryRewriter {
        final String NAME = "composite";
        private final Map<String, SimpleQueryRewriter> simpleQueryRewriters;

        private CompositeSimpleQueryRewriter(Map<String, SimpleQueryRewriter> simpleQueryRewriters) {
            this.simpleQueryRewriters = simpleQueryRewriters;
        }

        @Override
        public String getName() {
            return NAME;
        }

        @Override
        public QueryBuilder rewrite(QueryBuilder queryBuilder) {
            SimpleQueryRewriter rewriter = simpleQueryRewriters.get(queryBuilder.getName());
            if (rewriter != null) {
                return rewriter.rewrite(queryBuilder);
            }
            return queryBuilder;
        }
    }

    class NoOpSimpleQueryRewriter implements SimpleQueryRewriter {
        @Override
        public QueryBuilder rewrite(QueryBuilder queryBuilder) {
            return queryBuilder;
        }

        @Override
        public String getName() {
            return null;
        }
    }
}
