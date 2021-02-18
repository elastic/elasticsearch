/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
