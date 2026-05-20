/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.xpack.core.esql.DataSourceRequestInfo;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.permission.Role;

import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;
import static org.elasticsearch.xpack.security.authz.RBACEngine.maybeGetRBACEngineRole;

/**
 * Authorizes {@code global.data_source} for {@link EsqlDatasetActionNames#ESQL_PUT_DATASET_ACTION_NAME} when the request
 * advertises a separate datasource cluster action via {@link DataSourceRequestInfo#dataSourceClusterActionName()}.
 */
public class DatasetDatasourceRequestInterceptor implements RequestInterceptor {

    @Override
    public SubscribableListener<Void> intercept(
        RequestInfo requestInfo,
        AuthorizationEngine authorizationEngine,
        AuthorizationInfo authorizationInfo
    ) {
        if (requestInfo.getRequest() instanceof DataSourceRequestInfo dsi
            && EsqlDatasetActionNames.ESQL_PUT_DATASET_ACTION_NAME.equals(requestInfo.getAction())
            && dsi.dataSourceClusterActionName().equals(requestInfo.getAction()) == false) {
            Role role = maybeGetRBACEngineRole(authorizationInfo);
            if (role == null) {
                return SubscribableListener.nullSuccess();
            }
            String datasourceAction = dsi.dataSourceClusterActionName();
            if (role.checkClusterAction(datasourceAction, requestInfo.getRequest(), requestInfo.getAuthentication()) == false) {
                String user = requestInfo.getAuthentication().getEffectiveSubject().getUser().principal();
                return SubscribableListener.newFailed(
                    authorizationError("action [" + datasourceAction + "] is unauthorized for user [" + user + "]")
                );
            }
        }
        return SubscribableListener.nullSuccess();
    }
}
