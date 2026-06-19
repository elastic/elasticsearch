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
 * Authorizes {@code global.data_source} for {@link EsqlDatasetActionNames#ESQL_PUT_DATASET_ACTION_NAME} and
 * {@link EsqlDatasetActionNames#ESQL_RESOLVE_DATASET_ACTION_NAME} when the request advertises a separate datasource
 * cluster action via {@link DataSourceRequestInfo#dataSourceClusterActionName()}. PUT and the query-path read resolve
 * thus enforce the same dual-axis model: the standard filter checks the index privilege on the dataset name, this
 * interceptor checks {@code global.data_source} on the parent datasource.
 */
public class DatasetDatasourceRequestInterceptor implements RequestInterceptor {

    @Override
    public SubscribableListener<Void> intercept(
        RequestInfo requestInfo,
        AuthorizationEngine authorizationEngine,
        AuthorizationInfo authorizationInfo
    ) {
        if (requestInfo.getRequest() instanceof DataSourceRequestInfo dsi
            && appliesToAction(requestInfo.getAction(), dsi)
            && dsi.dataSourceClusterActionName().equals(requestInfo.getAction()) == false) {
            Role role = maybeGetRBACEngineRole(authorizationInfo);
            // Custom AuthorizationEngine implementations do not use RBAC Role; datasource policy is enforced there instead.
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

    private static boolean appliesToAction(String action, DataSourceRequestInfo dsi) {
        if (EsqlDatasetActionNames.ESQL_PUT_DATASET_ACTION_NAME.equals(action)) {
            return true;
        }
        // On the resolve path the filter has already narrowed the request to authorized datasets, so
        // dataSourceNames() is their parents; empty means none survived — nothing to authorize (PUT always has one).
        return EsqlDatasetActionNames.ESQL_RESOLVE_DATASET_ACTION_NAME.equals(action) && dsi.dataSourceNames().length > 0;
    }
}
