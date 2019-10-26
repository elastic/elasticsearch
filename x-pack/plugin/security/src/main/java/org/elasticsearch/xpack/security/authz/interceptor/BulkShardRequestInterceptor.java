/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

/**
 * Similar to {@link UpdateRequestInterceptor}, but checks if there are update requests embedded in a bulk request.
 */
public class BulkShardRequestInterceptor implements RequestInterceptor {

    private static final Logger logger = LogManager.getLogger(BulkShardRequestInterceptor.class);

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;

    public BulkShardRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        this.threadContext = threadPool.getThreadContext();
        this.licenseState = licenseState;
    }

    @Override
    public void intercept(RequestInfo requestInfo, AuthorizationEngine authzEngine, AuthorizationInfo authorizationInfo,
                          ActionListener<Void> listener) {
        if (requestInfo.getRequest() instanceof BulkShardRequest && licenseState.isDocumentAndFieldLevelSecurityAllowed()) {
            IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);

            final BulkShardRequest bulkShardRequest = (BulkShardRequest) requestInfo.getRequest();
            for (BulkItemRequest bulkItemRequest : bulkShardRequest.items()) {
                IndicesAccessControl.IndexAccessControl indexAccessControl =
                    indicesAccessControl.getIndexPermissions(bulkItemRequest.index());
                boolean found = false;
                if (indexAccessControl != null) {
                    boolean fls = indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
                    boolean dls = indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions();
                    if (fls || dls) {
                        if (bulkItemRequest.request() instanceof UpdateRequest) {
                            found = true;
                            logger.trace("aborting bulk item update request for index [{}]", bulkItemRequest.index());
                            bulkItemRequest.abort(bulkItemRequest.index(), new ElasticsearchSecurityException("Can't execute a bulk " +
                                "item request with update requests embedded if field or document level security is enabled",
                                RestStatus.BAD_REQUEST));
                        }
                    }
                }

                if (found == false) {
                    logger.trace("intercepted bulk request for index [{}] without any update requests, continuing execution",
                        bulkItemRequest.index());
                }
            }
        }
        listener.onResponse(null);
    }
}
