/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryRequest;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Map;

public class ValidateRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    public ValidateRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        super(threadPool.getThreadContext(), licenseState);
    }

    @Override
    void disableFeatures(
        IndicesRequest indicesRequest,
        Map<String, IndicesAccessControl.IndexAccessControl> indexAccessControlByIndex,
        ActionListener<Void> listener
    ) {
        final ValidateQueryRequest request = (ValidateQueryRequest) indicesRequest;
        if (indexAccessControlByIndex.values().stream().anyMatch(iac -> iac.getDocumentPermissions().hasDocumentLevelPermissions())) {
            if (hasRewrite(request)) {
                listener.onFailure(
                    new ElasticsearchSecurityException(
                        "Validate with rewrite isn't supported if document level security is enabled",
                        RestStatus.BAD_REQUEST
                    )
                );
            } else {
                listener.onResponse(null);
            }
        } else {
            listener.onResponse(null);
        }
    }

    @Override
    public boolean supports(IndicesRequest request) {
        if (request instanceof ValidateQueryRequest validateQueryRequest) {
            return hasRewrite(validateQueryRequest);
        } else {
            return false;
        }
    }

    private static boolean hasRewrite(ValidateQueryRequest validateQueryRequest) {
        return validateQueryRequest.rewrite();
    }
}
