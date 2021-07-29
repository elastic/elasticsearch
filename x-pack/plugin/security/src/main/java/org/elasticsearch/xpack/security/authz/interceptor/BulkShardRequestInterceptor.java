/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.core.MemoizedSupplier;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
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
        if (requestInfo.getRequest() instanceof BulkShardRequest && licenseState.isSecurityEnabled()) {
            IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
            BulkShardRequest bulkShardRequest = (BulkShardRequest) requestInfo.getRequest();
            // this uses the {@code BulkShardRequest#index()} because the {@code bulkItemRequest#index()}
            // can still be an unresolved date math expression
            IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(bulkShardRequest.index());
            // TODO replace if condition with assertion
            if (indexAccessControl != null) {
                MemoizedSupplier<Boolean> licenseChecker =
                        new MemoizedSupplier<>(() -> licenseState.checkFeature(Feature.SECURITY_DLS_FLS));
                for (BulkItemRequest bulkItemRequest : bulkShardRequest.items()) {
                    boolean found = false;
                    if (bulkItemRequest.request() instanceof UpdateRequest) {
                        boolean fls = indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
                        boolean dls = indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions();
                        // the feature usage checker is a "last-ditch" verification, it doesn't have practical importance
                        if ((fls || dls) && licenseChecker.get()) {
                            found = true;
                            logger.trace("aborting bulk item update request for index [{}]", bulkShardRequest.index());
                            bulkItemRequest.abort(bulkItemRequest.index(), new ElasticsearchSecurityException("Can't execute a bulk " +
                                    "item request with update requests embedded if field or document level security is enabled",
                                    RestStatus.BAD_REQUEST));
                        }
                    }
                    if (found == false) {
                        logger.trace("intercepted bulk request for index [{}] without any update requests, continuing execution",
                                bulkShardRequest.index());
                    }
                }
            }
        }
        listener.onResponse(null);
    }
}
