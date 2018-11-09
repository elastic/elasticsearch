/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.interceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.BulkShardRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.Role;

/**
 * Similar to {@link UpdateRequestInterceptor}, but checks if there are update requests embedded in a bulk request.
 */
public class BulkShardRequestInterceptor implements RequestInterceptor<BulkShardRequest> {

    private static final Logger logger = LogManager.getLogger(BulkShardRequestInterceptor.class);

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;

    public BulkShardRequestInterceptor(ThreadPool threadPool, XPackLicenseState licenseState) {
        this.threadContext = threadPool.getThreadContext();
        this.licenseState = licenseState;
    }

    @Override
    public void intercept(BulkShardRequest request, Authentication authentication, Role userPermissions, String action) {
        if (licenseState.isDocumentAndFieldLevelSecurityAllowed()) {
            IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);

            for (BulkItemRequest bulkItemRequest : request.items()) {
                IndicesAccessControl.IndexAccessControl indexAccessControl =
                    indicesAccessControl.getIndexPermissions(bulkItemRequest.index());
                if (indexAccessControl != null) {
                    boolean fls = indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
                    boolean dls = indexAccessControl.getQueries() != null;
                    if (fls || dls) {
                        if (bulkItemRequest.request() instanceof UpdateRequest) {
                            throw new ElasticsearchSecurityException("Can't execute a bulk request with update requests embedded if " +
                                "field or document level security is enabled", RestStatus.BAD_REQUEST);
                        }
                    }
                }
                logger.trace("intercepted bulk request for index [{}] without any update requests, continuing execution",
                    bulkItemRequest.index());
            }
        }
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof BulkShardRequest;
    }
}
