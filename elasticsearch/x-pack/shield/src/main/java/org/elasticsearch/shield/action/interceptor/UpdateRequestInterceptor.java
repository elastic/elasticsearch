/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

/**
 * A request interceptor that fails update request if field or document level security is enabled.
 * <p>
 * It can be dangerous for users if document where to be update via a role that has fls or dls enabled,
 * because only the fields that a role can see would be used to perform the update and without knowing the user may
 * remove the other fields, not visible for him, from the document being updated.
 */
public class UpdateRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor<UpdateRequest> {

    @Inject
    public UpdateRequestInterceptor(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool.getThreadContext());
    }

    @Override
    protected void disableFeatures(UpdateRequest updateRequest) {
        throw new ElasticsearchSecurityException("Can't execute an update request if field or document level security is enabled",
                RestStatus.BAD_REQUEST);
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof UpdateRequest;
    }
}
