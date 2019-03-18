/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexAction;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.dataframe.DataFrameMessages;
import org.elasticsearch.xpack.core.dataframe.action.PutDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.PutDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.PutDataFrameTransformAction.Response;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.support.Exceptions;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.persistence.DataframeIndex;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

public class TransportPutDataFrameTransformAction
        extends TransportMasterNodeAction<PutDataFrameTransformAction.Request, PutDataFrameTransformAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportPutDataFrameTransformAction.class);

    private final XPackLicenseState licenseState;
    private final Client client;
    private final DataFrameTransformsConfigManager dataFrameTransformsConfigManager;
    private final SecurityContext securityContext;

    @Inject
    public TransportPutDataFrameTransformAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                                ClusterService clusterService, XPackLicenseState licenseState,
                                                PersistentTasksService persistentTasksService,
                                                DataFrameTransformsConfigManager dataFrameTransformsConfigManager, Client client) {
        super(PutDataFrameTransformAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                PutDataFrameTransformAction.Request::new);
        this.licenseState = licenseState;
        this.client = client;
        this.dataFrameTransformsConfigManager = dataFrameTransformsConfigManager;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings) ?
            new SecurityContext(settings, threadPool.getThreadContext()) : null;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDataFrameTransformAction.Response newResponse() {
        return new PutDataFrameTransformAction.Response();
    }

    @Override
    protected void masterOperation(Request request, ClusterState clusterState, ActionListener<Response> listener) throws Exception {

        if (!licenseState.isDataFrameAllowed()) {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.DATA_FRAME));
            return;
        }

        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

        // set headers to run data frame transform as calling user
        Map<String, String> filteredHeaders = threadPool.getThreadContext().getHeaders().entrySet().stream()
                    .filter(e -> ClientHelper.SECURITY_HEADER_FILTERS.contains(e.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        DataFrameTransformConfig config = request.getConfig();
        config.setHeaders(filteredHeaders);

        String transformId = config.getId();
        // quick check whether a transform has already been created under that name
        if (PersistentTasksCustomMetaData.getTaskWithId(clusterState, transformId) != null) {
            listener.onFailure(new ResourceAlreadyExistsException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_TRANSFORM_EXISTS, transformId)));
            return;
        }

        String[] dest = indexNameExpressionResolver.concreteIndexNames(clusterState,
            IndicesOptions.lenientExpandOpen(),
            config.getDestination());

        if (dest.length > 0) {
            listener.onFailure(new ElasticsearchStatusException(
                DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_DEST_INDEX_ALREADY_EXISTS, config.getDestination()),
                RestStatus.BAD_REQUEST));
            return;
        }

        String[] src = indexNameExpressionResolver.concreteIndexNames(clusterState,
            IndicesOptions.lenientExpandOpen(),
            config.getSource());
        if (src.length == 0) {
            listener.onFailure(new ElasticsearchStatusException(
                DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_SOURCE_INDEX_MISSING, config.getSource()),
                RestStatus.BAD_REQUEST));
            return;
        }

        // Early check to verify that the user can create the destination index and can read from the source
        if (licenseState.isAuthAllowed()) {
            final String username = securityContext.getUser().principal();
            RoleDescriptor.IndicesPrivileges sourceIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                .indices(config.getSource())
                .privileges("read")
                .build();
            RoleDescriptor.IndicesPrivileges destIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                .indices(config.getDestination())
                .privileges("read", "index", "create_index")
                .build();

            HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
            privRequest.applicationPrivileges(new RoleDescriptor.ApplicationResourcePrivileges[0]);
            privRequest.username(username);
            privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
            privRequest.indexPrivileges(sourceIndexPrivileges, destIndexPrivileges);

            ActionListener<HasPrivilegesResponse> privResponseListener = ActionListener.wrap(
                r -> handlePrivsResponse(username, config, r, listener),
                listener::onFailure);

            client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
        } else { // No security enabled, just create the transform
            putDataFrame(config, listener);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutDataFrameTransformAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void handlePrivsResponse(String username,
                                     DataFrameTransformConfig config,
                                     HasPrivilegesResponse privilegesResponse,
                                     ActionListener<Response> listener) throws IOException {
        if (privilegesResponse.isCompleteMatch()) {
            putDataFrame(config, listener);
        } else {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            for (ResourcePrivileges index : privilegesResponse.getIndexPrivileges()) {
                builder.field(index.getResource());
                builder.map(index.getPrivileges());
            }
            builder.endObject();

            listener.onFailure(Exceptions.authorizationError("Cannot create data frame transform [{}]" +
                    " because user {} lacks permissions on the indices: {}",
                config.getId(), username, Strings.toString(builder)));
        }
    }

    private void putDataFrame(DataFrameTransformConfig config, ActionListener<Response> listener) {

        final Pivot pivot = new Pivot(config.getSource(), config.getQueryConfig().getQuery(), config.getPivotConfig());


        // <5> Return the listener, or clean up destination index on failure.
        ActionListener<Boolean> putTransformConfigurationListener = ActionListener.wrap(
            putTransformConfigurationResult -> listener.onResponse(new Response(true)),
            putTransformConfigurationException ->
                ClientHelper.executeAsyncWithOrigin(client,
                    ClientHelper.DATA_FRAME_ORIGIN,
                    DeleteIndexAction.INSTANCE,
                    new DeleteIndexRequest(config.getDestination()), ActionListener.wrap(
                        deleteIndexResponse -> listener.onFailure(putTransformConfigurationException),
                        deleteIndexException -> {
                            String msg = "Failed to delete destination index after creating transform [" + config.getId() + "] failed";
                            listener.onFailure(
                                new ElasticsearchStatusException(msg,
                                    RestStatus.INTERNAL_SERVER_ERROR,
                                    putTransformConfigurationException));
                        })
                )
        );

        // <4> Put our transform
        ActionListener<Boolean> createDestinationIndexListener = ActionListener.wrap(
            createIndexResult -> dataFrameTransformsConfigManager.putTransformConfiguration(config, putTransformConfigurationListener),
            createDestinationIndexException -> listener.onFailure(
                new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_CREATE_DEST_INDEX,
                    createDestinationIndexException))
        );

        // <3> Create the destination index
        ActionListener<Map<String, String>> deduceMappingsListener = ActionListener.wrap(
            mappings -> DataframeIndex.createDestinationIndex(client, config, mappings, createDestinationIndexListener),
            deduceTargetMappingsException -> listener.onFailure(
                new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_DEDUCE_DEST_MAPPINGS,
                    deduceTargetMappingsException))
        );

        // <2> Deduce our mappings for the destination index
        ActionListener<Boolean> pivotValidationListener = ActionListener.wrap(
            validationResult -> pivot.deduceMappings(client, deduceMappingsListener),
            validationException -> listener.onFailure(
                new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION,
                    validationException))
        );

        // <1> Validate our pivot
        pivot.validate(client, pivotValidationListener);
    }
}
