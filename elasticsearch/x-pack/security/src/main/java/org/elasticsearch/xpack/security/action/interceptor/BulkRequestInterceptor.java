/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.authz.AuthorizationService;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;

/**
 * Similar to {@link UpdateRequestInterceptor}, but checks if there are update requests embedded in a bulk request.
 */
public class BulkRequestInterceptor extends AbstractComponent implements RequestInterceptor<BulkRequest> {

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;

    @Inject
    public BulkRequestInterceptor(Settings settings, ThreadPool threadPool, XPackLicenseState licenseState) {
        super(settings);
        this.threadContext = threadPool.getThreadContext();
        this.licenseState = licenseState;
    }

    public void intercept(BulkRequest request, User user) {
        if (licenseState.isDocumentAndFieldLevelSecurityAllowed() == false) {
            return;
        }
        IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationService.INDICES_PERMISSIONS_KEY);

        for (IndicesRequest indicesRequest : request.subRequests()) {
            for (String index : indicesRequest.indices()) {
                IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(index);
                if (indexAccessControl != null) {
                    boolean fls = indexAccessControl.getFieldPermissions().hasFieldLevelSecurity();
                    boolean dls = indexAccessControl.getQueries() != null;
                    if (fls || dls) {
                        if (indicesRequest instanceof UpdateRequest) {
                            throw new ElasticsearchSecurityException("Can't execute a bulk request with update requests embedded if " +
                                    "field or document level security is enabled", RestStatus.BAD_REQUEST);
                        }
                    }
                }
                logger.trace("intercepted bulk request for index [{}] without any update requests, continuing execution", index);
            }
        }
    }

    @Override
    public boolean supports(TransportRequest request) {
        return request instanceof BulkRequest;
    }
}
