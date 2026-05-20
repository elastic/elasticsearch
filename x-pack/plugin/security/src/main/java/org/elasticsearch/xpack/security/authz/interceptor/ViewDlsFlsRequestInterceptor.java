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
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;

import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_VALUE;

/**
 * An interceptor which checks if the requested views or datasets have any DLS or FLS permissions applied.
 * If so, then the request is rejected, because views and datasets are not compatible with DLS or FLS.
 */
public class ViewDlsFlsRequestInterceptor implements RequestInterceptor {
    private static final Logger logger = LogManager.getLogger(ViewDlsFlsRequestInterceptor.class);

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
        if (requestInfo.getRequest() instanceof IndicesRequest indicesRequest
            && isInterceptorApplicable(indicesRequest, requestInfo.getAction())) {

            final ProjectMetadata projectMetadata = projectMetadataSupplier.get();
            List<String> requestedViews = Arrays.stream(indicesRequest.indices()).filter(projectMetadata::hasView).toList();
            List<String> requestedDatasets = Arrays.stream(indicesRequest.indices()).filter(projectMetadata::hasDataset).toList();

            if (requestedViews.isEmpty() == false || requestedDatasets.isEmpty() == false) {
                final IndicesAccessControl indicesAccessControl = INDICES_PERMISSIONS_VALUE.get(threadContext);
                if (indicesAccessControl != null) {
                    List<String> viewsWithDlsOrFls = requestedViews.stream().filter(view -> {
                        var indexAccessControl = indicesAccessControl.getIndexPermissions(view);
                        return indexAccessControl != null
                            && (indexAccessControl.getFieldPermissions().hasFieldLevelSecurity()
                                || indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions());
                    }).toList();

                    List<String> datasetsWithDlsOrFls = requestedDatasets.stream().filter(dataset -> {
                        var indexAccessControl = indicesAccessControl.getIndexPermissions(dataset);
                        return indexAccessControl != null
                            && (indexAccessControl.getFieldPermissions().hasFieldLevelSecurity()
                                || indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions());
                    }).toList();

                    if (viewsWithDlsOrFls.isEmpty() == false || datasetsWithDlsOrFls.isEmpty() == false) {
                        logger.debug(
                            "User [{}] requested views or datasets with DLS or FLS: views={} datasets={}",
                            requestInfo.getAuthentication(),
                            viewsWithDlsOrFls,
                            datasetsWithDlsOrFls
                        );
                        ElasticsearchSecurityException dlsFlsException = getDlsFlsException(viewsWithDlsOrFls, datasetsWithDlsOrFls);
                        return SubscribableListener.newFailed(dlsFlsException);
                    }
                }
            }
        }
        return SubscribableListener.nullSuccess();
    }

    private boolean isInterceptorApplicable(IndicesRequest indicesRequest, String action) {
        var indexAbstractionOptions = indicesRequest.indicesOptions().indexAbstractionOptions();
        return (indexAbstractionOptions.resolveViews() || indexAbstractionOptions.resolveDatasets())
            && TransportActionProxy.isProxyAction(action) == false
            && indicesRequest.indices() != null
            // Checking whether role has FLS or DLS first before checking indicesAccessControl for efficiency
            // because indicesAccessControl can contain a long list of indices
            && DlsFlsInterceptorUtils.isCurrentRoleNullOrHasDlsFlsPermissions(threadContext);
    }

    private static ElasticsearchSecurityException getDlsFlsException(List<String> viewsWithDlsOrFls, List<String> datasetsWithDlsOrFls) {
        ElasticsearchSecurityException dlsFlsException = new ElasticsearchSecurityException(
            "Views and datasets with document or field level security restrictions are not supported."
                + " Remove DLS/FLS restrictions from the affected views or datasets in the role definition,"
                + " or exclude them from the request.",
            RestStatus.FORBIDDEN
        );
        if (viewsWithDlsOrFls.isEmpty() == false) {
            dlsFlsException.addMetadata("es.views_with_dls_or_fls", viewsWithDlsOrFls);
        }
        if (datasetsWithDlsOrFls.isEmpty() == false) {
            dlsFlsException.addMetadata("es.datasets_with_dls_or_fls", datasetsWithDlsOrFls);
        }
        return dlsFlsException;
    }
}
