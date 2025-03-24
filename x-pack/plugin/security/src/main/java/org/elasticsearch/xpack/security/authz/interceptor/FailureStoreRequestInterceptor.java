/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Map;

public class FailureStoreRequestInterceptor extends FieldAndDocumentLevelSecurityRequestInterceptor {

    public FailureStoreRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        super(threadPool.getThreadContext(), licenseState);
    }

    @Override
    void disableFeatures(
        IndicesRequest indicesRequest,
        Map<String, IndicesAccessControl.IndexAccessControl> indicesAccessControlByIndex,
        ActionListener<Void> listener
    ) {
        for (var indexAccessControl : indicesAccessControlByIndex.entrySet()) {
            if (hasFailuresSelectorSuffix(indexAccessControl.getKey()) && hasDlsFlsPermissions(indexAccessControl.getValue())) {
                listener.onFailure(
                    new ElasticsearchSecurityException(
                        "Failure store access is not allowed for users who have "
                            + "field or document level security enabled on one of the indices",
                        RestStatus.BAD_REQUEST
                    )
                );
                return;
            }
        }
        listener.onResponse(null);
    }

    @Override
    boolean supports(IndicesRequest request) {
        if (request.indicesOptions().allowSelectors()) {
            for (String index : request.indices()) {
                if (hasFailuresSelectorSuffix(index)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean hasFailuresSelectorSuffix(String name) {
        return IndexNameExpressionResolver.hasSelectorSuffix(name)
            && IndexComponentSelector.getByKey(
                IndexNameExpressionResolver.splitSelectorExpression(name).v2()
            ) == IndexComponentSelector.FAILURES;
    }

    private boolean hasDlsFlsPermissions(IndicesAccessControl.IndexAccessControl indexAccessControl) {
        return indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions()
            || indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
    }

}
