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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.xpack.core.esql.EsqlDatasetActionNames;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.permission.Role;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.INDICES_PERMISSIONS_VALUE;
import static org.elasticsearch.xpack.security.authz.RBACEngine.maybeGetRBACEngineRole;

/**
 * An interceptor which checks if the requested views or datasets have any DLS or FLS permissions applied.
 * If so, then the request is rejected, because views and datasets are not compatible with DLS or FLS.
 * <p>
 * A dataset a query defines rather than names is covered too, and has to be asked about differently. Its read is
 * authorized under the name of the data source it reads through, which is not an index abstraction, and
 * {@code IndicesPermission} records neither DLS nor FLS for a name that resolves to nothing — so the access control
 * this reads for a registered name is unrestricted there whatever the role says. For that action the role's own
 * privilege groups are consulted instead.
 */
public class ViewAndDatasetDlsFlsRequestInterceptor implements RequestInterceptor {
    private static final Logger logger = LogManager.getLogger(ViewAndDatasetDlsFlsRequestInterceptor.class);

    static final String VIEWS_WITH_DLS_OR_FLS_METADATA_KEY = "es.views_with_dls_or_fls";
    static final String DATASETS_WITH_DLS_OR_FLS_METADATA_KEY = "es.datasets_with_dls_or_fls";

    // Two alternative user-facing messages: one when only views or only datasets are affected, one when both are.
    private static final String DLS_FLS_VIEWS_OR_DATASETS_MESSAGE =
        "%s with document or field level security restrictions are not supported."
            + " Remove DLS/FLS restrictions from the affected %s in the role definition, or exclude them from the request.";
    private static final String DLS_FLS_VIEWS_AND_DATASETS_MESSAGE = "Views and datasets with document or field level security"
        + " restrictions are not supported. See views_with_dls_or_fls and datasets_with_dls_or_fls for the affected names.";

    private final ThreadContext threadContext;
    private final Supplier<ProjectMetadata> projectMetadataSupplier;

    public ViewAndDatasetDlsFlsRequestInterceptor(ThreadContext threadContext, Supplier<ProjectMetadata> projectMetadataSupplier) {
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
            && EsqlDatasetActionNames.ESQL_AUTHORIZE_DATASOURCE_ACTION_NAME.equals(requestInfo.getAction())) {
            return interceptInlineDatasetRead(requestInfo, indicesRequest, authorizationInfo);
        }
        if (requestInfo.getRequest() instanceof IndicesRequest indicesRequest
            && isInterceptorApplicable(indicesRequest, requestInfo.getAction())) {

            final ProjectMetadata projectMetadata = projectMetadataSupplier.get();
            List<String> requestedViews = Arrays.stream(indicesRequest.indices()).filter(projectMetadata::hasView).toList();
            List<String> requestedDatasets = Arrays.stream(indicesRequest.indices()).filter(projectMetadata::hasDataset).toList();

            if (requestedViews.isEmpty() == false || requestedDatasets.isEmpty() == false) {
                final IndicesAccessControl indicesAccessControl = INDICES_PERMISSIONS_VALUE.get(threadContext);
                if (indicesAccessControl != null) {
                    List<String> viewsWithDlsOrFls = indicesWithDlsOrFls(requestedViews, indicesAccessControl);
                    List<String> datasetsWithDlsOrFls = indicesWithDlsOrFls(requestedDatasets, indicesAccessControl);

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

    /**
     * Refuses an inline dataset read whose authorized name is covered by a privilege group carrying DLS or FLS. The
     * group's own patterns are matched rather than the resolved access control, for the reason in the class javadoc;
     * a group that restricts documents or fields anywhere over that name is enough, because an external read applies
     * neither and would otherwise serve the whole resource.
     */
    private SubscribableListener<Void> interceptInlineDatasetRead(
        AuthorizationEngine.RequestInfo requestInfo,
        IndicesRequest indicesRequest,
        AuthorizationEngine.AuthorizationInfo authorizationInfo
    ) {
        // A custom AuthorizationEngine does not use an RBAC Role; DLS/FLS compatibility is enforced there instead.
        Role role = maybeGetRBACEngineRole(authorizationInfo);
        if (role == null || indicesRequest.indices() == null) {
            return SubscribableListener.nullSuccess();
        }
        List<String> restricted = new ArrayList<>();
        for (String name : indicesRequest.indices()) {
            if (anyGroupRestrictsDocumentsOrFields(role, name)) {
                restricted.add(name);
            }
        }
        if (restricted.isEmpty()) {
            return SubscribableListener.nullSuccess();
        }
        logger.debug(
            "User [{}] requested an inline dataset read under a DLS or FLS grant: {}",
            requestInfo.getAuthentication(),
            restricted
        );
        return SubscribableListener.newFailed(getDlsFlsException(List.of(), restricted));
    }

    private static boolean anyGroupRestrictsDocumentsOrFields(Role role, String name) {
        for (IndicesPermission.Group group : role.indices().groups()) {
            if (Regex.simpleMatch(group.indices(), name) == false) {
                continue;
            }
            if (group.getQuery() != null || group.getFieldPermissions().hasFieldLevelSecurity()) {
                return true;
            }
        }
        return false;
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

    private static List<String> indicesWithDlsOrFls(List<String> names, IndicesAccessControl indicesAccessControl) {
        return names.stream().filter(name -> hasDlsOrFls(indicesAccessControl.getIndexPermissions(name))).toList();
    }

    private static boolean hasDlsOrFls(IndicesAccessControl.IndexAccessControl indexAccessControl) {
        return indexAccessControl != null
            && (indexAccessControl.getFieldPermissions().hasFieldLevelSecurity()
                || indexAccessControl.getDocumentPermissions().hasDocumentLevelPermissions());
    }

    private static ElasticsearchSecurityException getDlsFlsException(List<String> viewsWithDlsOrFls, List<String> datasetsWithDlsOrFls) {
        final boolean hasViews = viewsWithDlsOrFls.isEmpty() == false;
        final boolean hasDatasets = datasetsWithDlsOrFls.isEmpty() == false;
        if (hasViews && hasDatasets) {
            ElasticsearchSecurityException exception = new ElasticsearchSecurityException(
                DLS_FLS_VIEWS_AND_DATASETS_MESSAGE,
                RestStatus.FORBIDDEN
            );
            exception.addMetadata(VIEWS_WITH_DLS_OR_FLS_METADATA_KEY, viewsWithDlsOrFls);
            exception.addMetadata(DATASETS_WITH_DLS_OR_FLS_METADATA_KEY, datasetsWithDlsOrFls);
            return exception;
        } else if (hasViews) {
            ElasticsearchSecurityException exception = new ElasticsearchSecurityException(
                format(DLS_FLS_VIEWS_OR_DATASETS_MESSAGE, "Views", "views"),
                RestStatus.FORBIDDEN
            );
            exception.addMetadata(VIEWS_WITH_DLS_OR_FLS_METADATA_KEY, viewsWithDlsOrFls);
            return exception;
        } else {
            ElasticsearchSecurityException exception = new ElasticsearchSecurityException(
                format(DLS_FLS_VIEWS_OR_DATASETS_MESSAGE, "Datasets", "datasets"),
                RestStatus.FORBIDDEN
            );
            exception.addMetadata(DATASETS_WITH_DLS_OR_FLS_METADATA_KEY, datasetsWithDlsOrFls);
            return exception;
        }
    }
}
