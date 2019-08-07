/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.PutDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.PutDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.SourceDestValidator;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TransportPutDataFrameTransformAction extends TransportMasterNodeAction<Request, AcknowledgedResponse> {

    private final XPackLicenseState licenseState;
    private final Client client;
    private final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;
    private final SecurityContext securityContext;
    private final DataFrameAuditor auditor;

    @Inject
    public TransportPutDataFrameTransformAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                                ClusterService clusterService, XPackLicenseState licenseState,
                                                DataFrameTransformsConfigManager dataFrameTransformsConfigManager, Client client,
                                                DataFrameAuditor auditor) {
        super(PutDataFrameTransformAction.NAME, transportService, clusterService, threadPool, actionFilters,
                PutDataFrameTransformAction.Request::new, indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.client = client;
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.auditor = auditor;
    }

    static HasPrivilegesRequest buildPrivilegeCheck(DataFrameTransformConfig config,
                                                    IndexNameExpressionResolver indexNameExpressionResolver,
                                                    ClusterState clusterState,
                                                    String username) {
        final String destIndex = config.getDestination().getIndex();
        final String[] concreteDest = indexNameExpressionResolver.concreteIndexNames(clusterState,
            IndicesOptions.lenientExpandOpen(),
            config.getDestination().getIndex());
        List<String> srcPrivileges = new ArrayList<>(2);
        srcPrivileges.add("read");

        List<String> destPrivileges = new ArrayList<>(3);
        destPrivileges.add("read");
        destPrivileges.add("index");
        // If the destination index does not exist, we can assume that we may have to create it on start.
        // We should check that the creating user has the privileges to create the index.
        if (concreteDest.length == 0) {
            destPrivileges.add("create_index");
            // We need to read the source indices mapping to deduce the destination mapping
            srcPrivileges.add("view_index_metadata");
        }
        RoleDescriptor.IndicesPrivileges destIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
            .indices(destIndex)
            .privileges(destPrivileges)
            .build();

        RoleDescriptor.IndicesPrivileges sourceIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
            .indices(config.getSource().getIndex())
            .privileges(srcPrivileges)
            .build();

        HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
        privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
        privRequest.username(username);
        privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
        privRequest.indexPrivileges(sourceIndexPrivileges, destIndexPrivileges);
        return privRequest;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState clusterState, ActionListener<AcknowledgedResponse> listener) {

        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

        // set headers to run data frame transform as calling user
        Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
                    .filter(e -> ClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        DataFrameTransformConfig config = request.getConfig()
            .setHeaders(filteredHeaders)
            .setCreateTime(Instant.now())
            .setVersion(Version.CURRENT);

        String transformId = config.getId();
        // quick check whether a transform has already been created under that name
        if (PersistentTasksCustomMetaData.getTaskWithId(clusterState, transformId) != null) {
            listener.onFailure(new ResourceAlreadyExistsException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_TRANSFORM_EXISTS, transformId)));
            return;
        }
        try {
            SourceDestValidator.validate(config, clusterState, indexNameExpressionResolver, request.isDeferValidation());
        } catch (ElasticsearchStatusException ex) {
            listener.onFailure(ex);
            return;
        }

        // Early check to verify that the user can create the destination index and can read from the source
        if (licenseState.isAuthAllowed() && request.isDeferValidation() == false) {
            final String username = securityContext.getUser().principal();
            HasPrivilegesRequest privRequest = buildPrivilegeCheck(config, indexNameExpressionResolver, clusterState, username);
            ActionListener<HasPrivilegesResponse> privResponseListener = ActionListener.wrap(
                r -> handlePrivsResponse(username, request, r, listener),
                listener::onFailure);

            client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
        } else { // No security enabled, just create the transform
            putDataFrame(request, listener);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataFrameTransformAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void handlePrivsResponse(String username,
                                     Request request,
                                     HasPrivilegesResponse privilegesResponse,
                                     ActionListener<AcknowledgedResponse> listener) {
        if (privilegesResponse.isCompleteMatch()) {
            putDataFrame(request, listener);
        } else {
            List<String> indices = privilegesResponse.getIndexPrivileges()
                .stream()
                .map(ResourcePrivileges::getResource)
                .collect(Collectors.toList());

            listener.onFailure(Exceptions.authorizationError(
                "Cannot create data frame transform [{}] because user {} lacks all the required permissions for indices: {}",
                request.getConfig().getId(),
                username,
                indices));
        }
    }

    private void putDataFrame(Request request, ActionListener<AcknowledgedResponse> listener) {

        final DataFrameTransformConfig config = request.getConfig();
        final Pivot pivot = new Pivot(config.getPivotConfig());

        // <3> Return to the listener
        ActionListener<Boolean> putTransformConfigurationListener = ActionListener.wrap(
            putTransformConfigurationResult -> {
                auditor.info(config.getId(), "Created data frame transform.");
                listener.onResponse(new AcknowledgedResponse(true));
            },
            listener::onFailure
        );

        // <2> Put our transform
        ActionListener<Boolean> pivotValidationListener = ActionListener.wrap(
            validationResult -> dataFrameTransformsConfigManager.putTransformConfiguration(config, putTransformConfigurationListener),
            validationException -> {
                if (validationException instanceof ElasticsearchStatusException) {
                    listener.onFailure(new ElasticsearchStatusException(
                        DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION,
                        ((ElasticsearchStatusException)validationException).status(),
                        validationException));
                } else {
                    listener.onFailure(new ElasticsearchStatusException(
                        DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION,
                        RestStatus.INTERNAL_SERVER_ERROR,
                        validationException));
                }
            }
        );

        try {
            pivot.validateConfig();
        } catch (ElasticsearchStatusException e) {
            listener.onFailure(new ElasticsearchStatusException(
                DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION,
                e.status(),
                e));
            return;
        } catch (Exception e) {
            listener.onFailure(new ElasticsearchStatusException(
                DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION, RestStatus.INTERNAL_SERVER_ERROR, e));
            return;
        }

        if (request.isDeferValidation()) {
            pivotValidationListener.onResponse(true);
        } else {
            pivot.validateQuery(client, config.getSource(), pivotValidationListener);
        }
    }
}
