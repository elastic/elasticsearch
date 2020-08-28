/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * A request interceptor that fails update request if field or document level security is enabled.
 * <p>
 * It can be dangerous for users if document where to be update via a role that has fls or dls enabled,
 * because only the fields that a role can see would be used to perform the update and without knowing the user may
 * remove the other fields, not visible for him, from the document being updated.
 */
public class UpdateRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    public UpdateRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        super(threadPool.getThreadContext(), licenseState);
    }

    @Override
    protected void disableFeatures(IndicesRequest updateRequest, boolean fieldLevelSecurityEnabled, boolean documentLevelSecurityEnabled,
                                   ActionListener<Void> listener) {
        listener.onFailure(new ElasticsearchSecurityException("Can't execute an update request if field or document level security " +
            "is enabled", RestStatus.BAD_REQUEST));
    }

    @Override
    public boolean supports(IndicesRequest request) {
        return request instanceof UpdateRequest;
    }
}
