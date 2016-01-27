/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.interceptor;

import org.elasticsearch.action.RealtimeRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

/**
 * If field level or document level security is enabled this interceptor disables the realtime feature of get, multi get, termsvector and
 * multi termsvector requests.
 */
public class RealtimeRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor<RealtimeRequest> {

    @Inject
    public RealtimeRequestInterceptor(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool.getThreadContext());
    }

    @Override
    protected void disableFeatures(RealtimeRequest realtimeRequest) {
        realtimeRequest.realtime(false);
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof RealtimeRequest;
    }
}
