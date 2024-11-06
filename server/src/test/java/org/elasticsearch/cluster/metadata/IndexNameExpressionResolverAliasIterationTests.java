/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.EmptySystemIndices;

public class IndexNameExpressionResolverAliasIterationTests extends IndexNameExpressionResolverTests {

    @Override
    protected IndexNameExpressionResolver createIndexNameExpressionResolver(ThreadContext threadContext) {
        return new IndexNameExpressionResolver(
            threadContext,
            EmptySystemIndices.INSTANCE,
            TestProjectResolvers.usingRequestHeader(threadContext)
        ) {
            @Override
            boolean iterateIndexAliases(int indexAliasesSize, int resolvedExpressionsSize) {
                return true;
            }
        };
    }

}
