/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.shield.User;
import org.elasticsearch.transport.TransportRequest;

/**
 * Simular to {@link UpdateRequestInterceptor}, but checks if there are update requests embedded in a bulk request.
 */
public class BulkRequestInterceptor extends FieldSecurityRequestInterceptor<BulkRequest> {

    @Inject
    public BulkRequestInterceptor(Settings settings) {
        super(settings);
    }

    @Override
    public void intercept(BulkRequest request, User user) {
        // FIXME remove this method override once we support bulk updates with DLS and FLS enabled overall. We'll still
        // need this interceptor because individual users may still have FLS/DLS enabled and we'll want to reject only
        // their requests. Also update the message to remove "document"
        if (ShieldPlugin.flsDlsEnabled(this.settings)) {
            disableFeatures(request);
        }
    }

    @Override
    protected void disableFeatures(BulkRequest bulkRequest) {
        for (ActionRequest actionRequest : bulkRequest.requests()) {
            if (actionRequest instanceof UpdateRequest) {
                throw new ElasticsearchSecurityException("Can't execute an bulk request with update requests embedded if document and field level security is enabled", RestStatus.BAD_REQUEST);
            }
        }
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof BulkRequest;
    }
}
