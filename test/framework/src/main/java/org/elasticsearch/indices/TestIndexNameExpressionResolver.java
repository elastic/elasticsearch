/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

/**
 * A utility class that simplifies the creation of {@link IndexNameExpressionResolver} instances in tests to avoid repetition of
 * creating the constructor arguments for a default instance.
 */
public class TestIndexNameExpressionResolver {

    private TestIndexNameExpressionResolver() {}

    /**
     * @return a new instance of a {@link IndexNameExpressionResolver} that has been created with a new {@link ThreadContext} and the
     * default {@link SystemIndices} instance
     */
    public static IndexNameExpressionResolver newInstance() {
        return new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY), EmptySystemIndices.INSTANCE);
    }

    /**
     * @return a new instance of a {@link IndexNameExpressionResolver} that has been created with the provided {@link ThreadContext} and
     * the default {@link SystemIndices} instance
     */
    public static IndexNameExpressionResolver newInstance(ThreadContext threadContext) {
        return new IndexNameExpressionResolver(threadContext, EmptySystemIndices.INSTANCE);
    }

    /**
     * @return a new instance of a {@link IndexNameExpressionResolver} that has been created with a new {@link ThreadContext} and
     * the provided {@link SystemIndices} instance
     */
    public static IndexNameExpressionResolver newInstance(SystemIndices systemIndices) {
        return new IndexNameExpressionResolver(new ThreadContext(Settings.EMPTY), systemIndices);
    }
}
