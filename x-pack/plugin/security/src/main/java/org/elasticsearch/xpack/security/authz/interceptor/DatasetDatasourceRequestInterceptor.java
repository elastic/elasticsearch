/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;

import java.util.Set;

import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.elasticsearch.xpack.security.authz.RBACEngine.maybeGetRBACEngineRole;

/**
 * Authorizes {@code global.data_source} for the actions that attach a dataset to a data source — registering one
 * ({@link EsqlDatasetActionNames#ESQL_PUT_DATASET_ACTION_NAME}) and reading one whose definition the query carries
 * ({@link EsqlDatasetActionNames#ESQL_AUTHORIZE_DATASOURCE_ACTION_NAME}) — when the request advertises a separate
 * datasource cluster action via {@link DataSourceRequestInfo#dataSourceClusterActionName()}. One interceptor covers
 * both because the datasource half is the same question, and asking it twice would let the answers drift.
 */
public class DatasetDatasourceRequestInterceptor implements RequestInterceptor {

    private static final Set<String> INTERCEPTED_ACTIONS = Set.of(
        EsqlDatasetActionNames.ESQL_PUT_DATASET_ACTION_NAME,
        EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASOURCE_ACTION_NAME
    );

    private final ThreadContext threadContext;
    private final AuditTrailService auditTrailService;

    public DatasetDatasourceRequestInterceptor(ThreadContext threadContext, AuditTrailService auditTrailService) {
        this.threadContext = threadContext;
        this.auditTrailService = auditTrailService;
    }

    @Override
    public SubscribableListener<Void> intercept(
        RequestInfo requestInfo,
        AuthorizationEngine authorizationEngine,
        AuthorizationInfo authorizationInfo
    ) {
        if (requestInfo.getRequest() instanceof DataSourceRequestInfo dsi
            && INTERCEPTED_ACTIONS.contains(requestInfo.getAction())
            && dsi.dataSourceClusterActionName().equals(requestInfo.getAction()) == false) {
            var auditTrail = auditTrailService.get();

            Role role = maybeGetRBACEngineRole(authorizationInfo);
            // Custom AuthorizationEngine implementations do not use RBAC Role; datasource policy is enforced there instead.
            if (role == null) {
                return SubscribableListener.nullSuccess();
            }
            String datasourceAction = dsi.dataSourceClusterActionName();
            if (role.checkClusterAction(datasourceAction, requestInfo.getRequest(), requestInfo.getAuthentication()) == false) {
                String user = requestInfo.getAuthentication().getEffectiveSubject().getUser().principal();
                auditTrail.accessDenied(
                    AuditUtil.extractRequestId(threadContext),
                    requestInfo.getAuthentication(),
                    requestInfo.getAction(),
                    requestInfo.getRequest(),
                    authorizationInfo
                );
                return SubscribableListener.newFailed(
                    authorizationError("action [" + datasourceAction + "] is unauthorized for user [" + user + "]")
                );
            }
        }
        return SubscribableListener.nullSuccess();
    }
}
