/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.interceptor;

import org.elasticsearch.action.fieldstats.FieldStatsRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

/**
 * Intercepts requests to shards to field level stats and strips fields that the user is not allowed to access from the response.
 */
public class FieldStatsRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor<FieldStatsRequest> {
    @Inject
    public FieldStatsRequestInterceptor(Settings settings, ThreadPool threadPool, XPackLicenseState licenseState) {
        super(settings, threadPool.getThreadContext(), licenseState);
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof FieldStatsRequest;
    }

    @Override
    protected void disableFeatures(FieldStatsRequest request, boolean fieldLevelSecurityEnabled, boolean documentLevelSecurityEnabled) {
        if (fieldLevelSecurityEnabled) {
            request.setUseCache(false);
        }
    }
}
