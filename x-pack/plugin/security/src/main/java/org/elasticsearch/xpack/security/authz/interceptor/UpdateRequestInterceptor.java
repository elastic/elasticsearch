/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Map;

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
    void disableFeatures(
        IndicesRequest indicesRequest,
        Map<String, IndicesAccessControl.IndexAccessControl> indicesAccessControlByIndex,
        ActionListener<Void> listener
    ) {
        listener.onFailure(
            new ElasticsearchSecurityException(
                "Can't execute an update request if field or document level security " + "is enabled",
                RestStatus.BAD_REQUEST
            )
        );
    }

    @Override
    String[] requestIndices(IndicesRequest indicesRequest) {
        if (indicesRequest instanceof UpdateRequest updateRequest) {
            if (updateRequest.getShardId() != null) {
                return new String[] { updateRequest.getShardId().getIndexName() };
            }
        }
        return new String[0];
    }

    @Override
    public boolean supports(IndicesRequest request) {
        return request instanceof UpdateRequest;
    }
}
