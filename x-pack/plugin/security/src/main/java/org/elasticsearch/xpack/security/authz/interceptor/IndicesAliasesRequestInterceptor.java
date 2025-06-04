/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.elasticsearch.xpack.core.security.SecurityField.DOCUMENT_LEVEL_SECURITY_FEATURE;
import static org.elasticsearch.xpack.core.security.SecurityField.FIELD_LEVEL_SECURITY_FEATURE;

public final class IndicesAliasesRequestInterceptor implements RequestInterceptor {

    private final ThreadContext threadContext;
    private final XPackLicenseState licenseState;
    private final AuditTrailService auditTrailService;
    private final boolean dlsFlsEnabled;

    public IndicesAliasesRequestInterceptor(
        ThreadContext threadContext,
        XPackLicenseState licenseState,
        AuditTrailService auditTrailService,
        boolean dlsFlsEnabled
    ) {
        this.threadContext = threadContext;
        this.licenseState = licenseState;
        this.auditTrailService = auditTrailService;
        this.dlsFlsEnabled = dlsFlsEnabled;
    }

    @Override
    public SubscribableListener<Void> intercept(
        RequestInfo requestInfo,
        AuthorizationEngine authorizationEngine,
        AuthorizationInfo authorizationInfo
    ) {
        if (requestInfo.getRequest() instanceof IndicesAliasesRequest request) {
            final AuditTrail auditTrail = auditTrailService.get();
            final boolean isDlsLicensed = DOCUMENT_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState);
            final boolean isFlsLicensed = FIELD_LEVEL_SECURITY_FEATURE.checkWithoutTracking(licenseState);
            IndicesAccessControl indicesAccessControl = threadContext.getTransient(AuthorizationServiceField.INDICES_PERMISSIONS_KEY);
            if (dlsFlsEnabled && (isDlsLicensed || isFlsLicensed)) {
                for (IndicesAliasesRequest.AliasActions aliasAction : request.getAliasActions()) {
                    if (aliasAction.actionType() == IndicesAliasesRequest.AliasActions.Type.ADD) {
                        for (String index : aliasAction.indices()) {
                            IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(index);
                            if (indexAccessControl != null
                                && (indexAccessControl.getFieldPermissions().hasFieldLevelSecurity()
                                    || indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions())) {
                                return SubscribableListener.newFailed(
                                    new ElasticsearchSecurityException(
                                        "Alias requests are not allowed for "
                                            + "users who have field or document level security enabled on one of the indices",
                                        RestStatus.BAD_REQUEST
                                    )
                                );
                            }
                        }
                    }
                }
            }

            Map<String, List<String>> indexToAliasesMap = request.getAliasActions()
                .stream()
                .filter(aliasAction -> aliasAction.actionType() == IndicesAliasesRequest.AliasActions.Type.ADD)
                .flatMap(
                    aliasActions -> Arrays.stream(aliasActions.indices())
                        .map(indexName -> new Tuple<>(indexName, Arrays.asList(aliasActions.aliases())))
                )
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2, (existing, toMerge) -> {
                    List<String> list = new ArrayList<>(existing.size() + toMerge.size());
                    list.addAll(existing);
                    list.addAll(toMerge);
                    return list;
                }));
            final SubscribableListener<Void> listener = new SubscribableListener<>();
            authorizationEngine.validateIndexPermissionsAreSubset(
                requestInfo,
                authorizationInfo,
                indexToAliasesMap,
                wrapPreservingContext(ActionListener.wrap(authzResult -> {
                    if (authzResult.isGranted()) {
                        // do not audit success again
                        listener.onResponse(null);
                    } else {
                        auditTrail.accessDenied(
                            AuditUtil.extractRequestId(threadContext),
                            requestInfo.getAuthentication(),
                            requestInfo.getAction(),
                            request,
                            authorizationInfo
                        );
                        listener.onFailure(
                            Exceptions.authorizationError(
                                "Adding an alias is not allowed when the alias " + "has more permissions than any of the indices"
                            )
                        );
                    }
                }, listener::onFailure), threadContext)
            );
            return listener;
        } else {
            return SubscribableListener.nullSuccess();
        }
    }
}
