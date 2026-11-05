/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.interceptor;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.RequestInfo;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.support.Automatons;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.security.audit.AuditTrail;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.audit.AuditUtil;
import org.elasticsearch.xpack.security.authz.RBACEngine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.action.support.ContextPreservingActionListener.wrapPreservingContext;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_VALUE;

public final class IndicesAliasesRequestInterceptor implements RequestInterceptor {

    private final ThreadContext threadContext;
    private final AuditTrailService auditTrailService;
    private final boolean dlsFlsEnabled;
    private final Function<String, String> parentDataStreamName;

    public IndicesAliasesRequestInterceptor(ThreadContext threadContext, AuditTrailService auditTrailService, boolean dlsFlsEnabled) {
        this(threadContext, auditTrailService, dlsFlsEnabled, indexName -> null);
    }

    /**
     * @param parentDataStreamName the name of the data stream that an index is a backing or failure index of, or {@code null}: a user
     *                             may be authorized for such an index through the name of its data stream alone
     */
    public IndicesAliasesRequestInterceptor(
        ThreadContext threadContext,
        AuditTrailService auditTrailService,
        boolean dlsFlsEnabled,
        Function<String, String> parentDataStreamName
    ) {
        this.threadContext = threadContext;
        this.auditTrailService = auditTrailService;
        this.dlsFlsEnabled = dlsFlsEnabled;
        this.parentDataStreamName = parentDataStreamName;
    }

    @Override
    public SubscribableListener<Void> intercept(
        RequestInfo requestInfo,
        AuthorizationEngine authorizationEngine,
        AuthorizationInfo authorizationInfo
    ) {
        if (requestInfo.getRequest() instanceof IndicesAliasesRequest request) {
            final AuditTrail auditTrail = auditTrailService.get();
            IndicesAccessControl indicesAccessControl = INDICES_PERMISSIONS_VALUE.get(threadContext);
            if (dlsFlsEnabled && DlsFlsInterceptorUtils.isCurrentRoleNullOrHasDlsFlsPermissions(threadContext)) {
                for (IndicesAliasesRequest.AliasActions aliasAction : request.getAliasActions()) {
                    // a rename moves the data of its source to a new name, which is what an alias does, so it is held to the same rule
                    // for the source; the new name is only a name, and what the user may do under it is checked below
                    if (aliasAction.actionType() == IndicesAliasesRequest.AliasActions.Type.ADD
                        || aliasAction.actionType() == IndicesAliasesRequest.AliasActions.Type.RENAME_INDEX) {
                        final String[] indices = aliasAction.actionType() == IndicesAliasesRequest.AliasActions.Type.RENAME_INDEX
                            ? new String[] { aliasAction.indices()[0] }
                            : aliasAction.indices();
                        for (String index : indices) {
                            IndicesAccessControl.IndexAccessControl indexAccessControl = indicesAccessControl.getIndexPermissions(index);
                            if (indexAccessControl != null
                                && (indexAccessControl.getFieldPermissions().hasFieldLevelSecurity()
                                    || indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions())) {
                                return SubscribableListener.newFailed(
                                    new ElasticsearchSecurityException(
                                        (aliasAction.actionType() == IndicesAliasesRequest.AliasActions.Type.RENAME_INDEX
                                            ? "Renaming an index is not allowed for "
                                            : "Alias requests are not allowed for ")
                                            + "users who have field or document level security enabled on one of the indices",
                                        RestStatus.BAD_REQUEST
                                    )
                                );
                            }
                        }
                    }
                }
            }

            // A rename moves the data to a name that may give the user more access than the source does, as an alias could: it must
            // not allow more than the source does. The user may be authorized for a backing index only through its data stream.
            final Role role = RBACEngine.maybeGetRBACEngineRole(authorizationInfo);
            final Map<String, List<String>> renamesForEngine = new HashMap<>();
            for (IndicesAliasesRequest.AliasActions aliasAction : request.getAliasActions()) {
                if (aliasAction.actionType() != IndicesAliasesRequest.AliasActions.Type.RENAME_INDEX) {
                    continue;
                }
                final String source = aliasAction.indices()[0];
                final String newName = aliasAction.newIndex();
                if (role == null) {
                    // a custom authorization engine decides by the names alone
                    renamesForEngine.computeIfAbsent(source, k -> new ArrayList<>()).add(newName);
                } else if (renameAllowsNoMoreThanSource(role, source, newName) == false) {
                    auditTrail.accessDenied(
                        AuditUtil.extractRequestId(threadContext),
                        requestInfo.getAuthentication(),
                        requestInfo.getAction(),
                        request,
                        authorizationInfo
                    );
                    return SubscribableListener.newFailed(
                        Exceptions.authorizationError(
                            "Renaming an index is not allowed when the new name has more permissions than the index"
                        )
                    );
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
            renamesForEngine.forEach((source, newNames) -> indexToAliasesMap.merge(source, newNames, (existing, toMerge) -> {
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
                                request.getAliasActions()
                                    .stream()
                                    .anyMatch(a -> a.actionType() == IndicesAliasesRequest.AliasActions.Type.ADD) == false
                                        ? "Renaming an index is not allowed when the new name has more permissions than the index"
                                        : "Adding an alias is not allowed when the alias " + "has more permissions than any of the indices"
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

    /**
     * Whether the role allows no more under the new name than it does for the index being renamed, counting for the index what it
     * is allowed through its data stream, if it has one. Moving the data to a name that allows more would expose it to the user in a
     * way they had no right to.
     */
    private boolean renameAllowsNoMoreThanSource(Role role, String source, String newName) {
        final List<Automaton> sourcePermissions = new ArrayList<>(2);
        sourcePermissions.add(role.allowedActionsMatcher(source));
        final String dataStream = parentDataStreamName.apply(source);
        if (dataStream != null) {
            sourcePermissions.add(role.allowedActionsMatcher(dataStream));
        }
        return Automatons.subsetOf(role.allowedActionsMatcher(newName), Automatons.unionAndMinimize(sourcePermissions));
    }
}
