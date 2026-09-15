/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack;

import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.interceptor.RestServerActionPlugin;
import org.elasticsearch.rest.RestInterceptor;

import java.util.List;

/**
 * Test-only plugin that lives in the {@code org.elasticsearch.xpack} package so that
 * {@code ActionModule#isInternalPlugin} accepts it. The restriction exists to prevent
 * third-party plugins from hooking into privileged extension points; the {@code xpack}
 * namespace is the canonical signal for "Elastic-internal" code.
 */
public final class InternalRestInterceptorPlugin implements RestServerActionPlugin {

    private final List<RestInterceptor> interceptors;

    public InternalRestInterceptorPlugin(List<RestInterceptor> interceptors) {
        this.interceptors = interceptors;
    }

    @Override
    public List<RestInterceptor> getRestHandlerInterceptors(ThreadContext threadContext) {
        return interceptors;
    }
}
