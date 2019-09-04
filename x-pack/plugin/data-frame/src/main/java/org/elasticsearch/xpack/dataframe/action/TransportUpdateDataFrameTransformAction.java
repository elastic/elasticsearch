/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.LoggerMessageFormat;
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
import org.elasticsearch.xpack.core.dataframe.action.UpdateDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.UpdateDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.UpdateDataFrameTransformAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfigUpdate;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.dataframe.notifications.DataFrameAuditor;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.persistence.DataframeIndex;
import org.elasticsearch.xpack.dataframe.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.dataframe.transforms.SourceDestValidator;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;

import java.io.IOException;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.dataframe.action.TransportPutDataFrameTransformAction.buildPrivilegeCheck;

public class TransportUpdateDataFrameTransformAction extends TransportMasterNodeAction<Request, Response> {

    private static final Logger logger = LogManager.getLogger(TransportUpdateDataFrameTransformAction.class);
    private final XPackLicenseState licenseState;
    private final Client client;
    private final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;
    private final SecurityContext securityContext;
    private final DataFrameAuditor auditor;

    @Inject
    public TransportUpdateDataFrameTransformAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                                   ClusterService clusterService, XPackLicenseState licenseState,
                                                   DataFrameTransformsConfigManager dataFrameTransformsConfigManager, Client client,
                                                   DataFrameAuditor auditor) {
        super(UpdateDataFrameTransformAction.NAME, transportService, clusterService, threadPool, actionFilters,
                Request::new, indexNameExpressionResolver);
        this.licenseState = licenseState;
        this.client = client;
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
        this.auditor = auditor;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected Response read(StreamInput in) throws IOException {
        return new Response(in);
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState clusterState, ActionListener<Response> listener) {

        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

        // set headers to run data frame transform as calling user
        Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
                    .filter(e -> ClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        DataFrameTransformConfigUpdate update = request.getUpdate();
        update.setHeaders(filteredHeaders);

        // GET transform and attempt to update
        // We don't want the update to complete if the config changed between GET and INDEX
        dataFrameTransformsConfigManager.getTransformConfigurationForUpdate(request.getId(), ActionListener.wrap(
            configAndVersion -> {
                final DataFrameTransformConfig config = configAndVersion.v1();
                // If it is a noop don't bother even writing the doc, save the cycles, just return here.
                if (update.isNoop(config)) {
                    listener.onResponse(new Response(config));
                    return;
                }
                DataFrameTransformConfig updatedConfig = update.apply(config);
                validateAndUpdateDataFrame(request, clusterState, updatedConfig, configAndVersion.v2(), listener);
            },
            listener::onFailure
        ));
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void handlePrivsResponse(String username,
                                     Request request,
                                     DataFrameTransformConfig config,
                                     SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
                                     ClusterState clusterState,
                                     HasPrivilegesResponse privilegesResponse,
                                     ActionListener<Response> listener) {
        if (privilegesResponse.isCompleteMatch()) {
            updateDataFrame(request, config, seqNoPrimaryTermAndIndex, clusterState, listener);
        } else {
            List<String> indices = privilegesResponse.getIndexPrivileges()
                .stream()
                .map(ResourcePrivileges::getResource)
                .collect(Collectors.toList());

            listener.onFailure(Exceptions.authorizationError(
                "Cannot update data frame transform [{}] because user {} lacks all the required permissions for indices: {}",
                request.getId(),
                username,
                indices));
        }
    }

    private void validateAndUpdateDataFrame(Request request,
                                            ClusterState clusterState,
                                            DataFrameTransformConfig config,
                                            SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
                                            ActionListener<Response> listener) {
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
                r -> handlePrivsResponse(username, request, config, seqNoPrimaryTermAndIndex, clusterState, r, listener),
                listener::onFailure);

            client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
        } else { // No security enabled, just create the transform
            updateDataFrame(request, config, seqNoPrimaryTermAndIndex, clusterState, listener);
        }
    }
    private void updateDataFrame(Request request,
                                 DataFrameTransformConfig config,
                                 SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex,
                                 ClusterState clusterState,
                                 ActionListener<Response> listener) {

        final Pivot pivot = new Pivot(config.getPivotConfig());

        // <3> Return to the listener
        ActionListener<Boolean> putTransformConfigurationListener = ActionListener.wrap(
            putTransformConfigurationResult -> {
                auditor.info(config.getId(), "updated data frame transform.");
                dataFrameTransformsConfigManager.deleteOldTransformConfigurations(request.getId(), ActionListener.wrap(
                    r -> {
                        logger.trace("[{}] successfully deleted old transform configurations", request.getId());
                        listener.onResponse(new Response(config));
                    },
                    e -> {
                        logger.warn(
                            LoggerMessageFormat.format("[{}] failed deleting old transform configurations.", request.getId()),
                            e);
                        listener.onResponse(new Response(config));
                    }
                ));
            },
            // If we failed to INDEX AND we created the destination index, the destination index will still be around
            // This is a similar behavior to _start
            listener::onFailure
        );

        // <2> Update our transform
        ActionListener<Void> createDestinationListener = ActionListener.wrap(
            createDestResponse -> dataFrameTransformsConfigManager.updateTransformConfiguration(config,
                seqNoPrimaryTermAndIndex,
                putTransformConfigurationListener),
            listener::onFailure
        );

        // <1> Create destination index if necessary
        ActionListener<Boolean> pivotValidationListener = ActionListener.wrap(
            validationResult -> {
                String[] dest = indexNameExpressionResolver.concreteIndexNames(clusterState,
                    IndicesOptions.lenientExpandOpen(),
                    config.getDestination().getIndex());
                String[] src = indexNameExpressionResolver.concreteIndexNames(clusterState,
                    IndicesOptions.lenientExpandOpen(),
                    config.getSource().getIndex());
                // If we are running, we should verify that the destination index exists and create it if it does not
                if (PersistentTasksCustomMetaData.getTaskWithId(clusterState, request.getId()) != null
                    && dest.length == 0
                    // Verify we have source indices. The user could defer_validations and if the task is already running
                    // we allow source indices to disappear. If the source and destination indices do not exist, don't do anything
                    // the transform will just have to dynamically create the destination index without special mapping.
                    && src.length > 0) {
                    createDestination(pivot, config, createDestinationListener);
                } else {
                    createDestinationListener.onResponse(null);
                }
            },
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

        // <0> Validate the pivot if necessary
        if (request.isDeferValidation()) {
            pivotValidationListener.onResponse(true);
        } else {
            pivot.validateQuery(client, config.getSource(), pivotValidationListener);
        }
    }

    private void createDestination(Pivot pivot, DataFrameTransformConfig config, ActionListener<Void> listener) {
        ActionListener<Map<String, String>> deduceMappingsListener = ActionListener.wrap(
            mappings -> DataframeIndex.createDestinationIndex(
                client,
                Clock.systemUTC(),
                config,
                mappings,
                ActionListener.wrap(r -> listener.onResponse(null), listener::onFailure)),
            deduceTargetMappingsException -> listener.onFailure(
                new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_DEDUCE_DEST_MAPPINGS,
                    deduceTargetMappingsException))
        );

        pivot.deduceMappings(client, config.getSource(), deduceMappingsListener);
    }
}
