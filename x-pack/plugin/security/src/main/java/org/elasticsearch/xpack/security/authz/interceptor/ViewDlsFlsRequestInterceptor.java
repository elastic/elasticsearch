/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.interceptor;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_VALUE;

/**
 * An interceptor which checks if the requested View has any DLS or FLS permissions applied.
 * If so, then the request is rejected, because Views are not compatible with DLS or FLS.
 */
public class ViewDlsFlsRequestInterceptor implements RequestInterceptor {
    private final ThreadContext threadContext;
    private final Supplier<ProjectMetadata> projectMetadataSupplier;

    public ViewDlsFlsRequestInterceptor(ThreadContext threadContext, Supplier<ProjectMetadata> projectMetadataSupplier) {
        this.threadContext = threadContext;
        this.projectMetadataSupplier = projectMetadataSupplier;
    }

    @Override
    public SubscribableListener<Void> intercept(
        AuthorizationEngine.RequestInfo requestInfo,
        AuthorizationEngine authorizationEngine,
        AuthorizationEngine.AuthorizationInfo authorizationInfo
    ) {
        if (requestInfo.getRequest() instanceof IndicesRequest.Replaceable indicesRequest
            && indicesRequest.indicesOptions().indexAbstractionOptions().resolveViews()
            && TransportActionProxy.isProxyAction(requestInfo.getAction()) == false
            && indicesRequest.indices() != null
            && indicesRequest.indices().length > 0) {

            final ProjectMetadata projectMetadata = projectMetadataSupplier.get();
            List<String> requestedViews = Arrays.stream(indicesRequest.indices()).filter(projectMetadata::hasView).toList();

            if (requestedViews.isEmpty() == false) {
                final IndicesAccessControl indicesAccessControl = INDICES_PERMISSIONS_VALUE.get(threadContext);
                Set<String> indicesWithDlsOrFls = new HashSet<>(indicesAccessControl.getIndicesWithFieldOrDocumentLevelSecurity());
                String viewsWithDlsOrFls = requestedViews.stream().filter(indicesWithDlsOrFls::contains).collect(Collectors.joining(","));

                if (viewsWithDlsOrFls.isEmpty() == false) {
                    ElasticsearchSecurityException dlsFlsException = getDlsFlsException(viewsWithDlsOrFls);
                    return SubscribableListener.newFailed(dlsFlsException);
                }
            }
        }
        return SubscribableListener.nullSuccess();
    }

    private static ElasticsearchSecurityException getDlsFlsException(String viewsWithDlsOrFls) {
        ElasticsearchSecurityException dlsFlsException = new ElasticsearchSecurityException(
            "The request contains views on which the querying user's role has DLS/FLS restrictions applied."
                + " Neither DLS nor FLS may be applied to views. Fix the permissions not to include the views,"
                + " or change the query, so that it doesn't include the affected views.",
            RestStatus.FORBIDDEN
        );
        dlsFlsException.addMetadata("es.views_with_dls_or_fls", viewsWithDlsOrFls);
        return dlsFlsException;
    }
}
