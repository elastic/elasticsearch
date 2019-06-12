/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceAlreadyExistsException;
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
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
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
import org.elasticsearch.xpack.dataframe.transforms.pivot.Pivot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TransportPutDataFrameTransformAction
        extends TransportMasterNodeAction<PutDataFrameTransformAction.Request, AcknowledgedResponse> {

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

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(Request request, ClusterState clusterState, ActionListener<AcknowledgedResponse> listener)
            throws Exception {

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
        final String destIndex = config.getDestination().getIndex();
        Set<String> concreteSourceIndexNames = new HashSet<>();
        for(String src : config.getSource().getIndex()) {
            String[] concreteNames = indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), src);
            if (concreteNames.length == 0) {
                listener.onFailure(new ElasticsearchStatusException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_SOURCE_INDEX_MISSING, src),
                    RestStatus.BAD_REQUEST));
                return;
            }
            if (Regex.simpleMatch(src, destIndex)) {
                listener.onFailure(new ElasticsearchStatusException(
                    DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_DEST_IN_SOURCE, destIndex, src),
                    RestStatus.BAD_REQUEST
                ));
                return;
            }
            concreteSourceIndexNames.addAll(Arrays.asList(concreteNames));
        }

        if (concreteSourceIndexNames.contains(destIndex)) {
            listener.onFailure(new ElasticsearchStatusException(
                DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_DEST_IN_SOURCE,
                    destIndex,
                    Strings.arrayToCommaDelimitedString(config.getSource().getIndex())),
                RestStatus.BAD_REQUEST
            ));
            return;
        }

        final String[] concreteDest =
            indexNameExpressionResolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), destIndex);

        if (concreteDest.length > 1) {
            listener.onFailure(new ElasticsearchStatusException(
                DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_DEST_SINGLE_INDEX, destIndex),
                RestStatus.BAD_REQUEST
            ));
            return;
        }
        if (concreteDest.length > 0 && concreteSourceIndexNames.contains(concreteDest[0])) {
            listener.onFailure(new ElasticsearchStatusException(
                DataFrameMessages.getMessage(DataFrameMessages.REST_PUT_DATA_FRAME_DEST_IN_SOURCE,
                    concreteDest[0],
                    Strings.arrayToCommaDelimitedString(concreteSourceIndexNames.toArray(new String[0]))),
                RestStatus.BAD_REQUEST
            ));
            return;
        }

        // Early check to verify that the user can create the destination index and can read from the source
        if (licenseState.isAuthAllowed()) {
            final String username = securityContext.getUser().principal();
            RoleDescriptor.IndicesPrivileges sourceIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                .indices(config.getSource().getIndex())
                .privileges("read")
                .build();
            List<String> destPrivileges = new ArrayList<>(3);
            destPrivileges.add("read");
            destPrivileges.add("index");
            // If the destination index does not exist, we can assume that we may have to create it on start.
            // We should check that the creating user has the privileges to create the index.
            if (concreteDest.length == 0) {
                destPrivileges.add("create_index");
            }
            RoleDescriptor.IndicesPrivileges destIndexPrivileges = RoleDescriptor.IndicesPrivileges.builder()
                .indices(destIndex)
                .privileges(destPrivileges)
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
                                     ActionListener<AcknowledgedResponse> listener) throws IOException {
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

    private void putDataFrame(DataFrameTransformConfig config, ActionListener<AcknowledgedResponse> listener) {

        final Pivot pivot = new Pivot(config.getPivotConfig());


        // <5> Return the listener, or clean up destination index on failure.
        ActionListener<Boolean> putTransformConfigurationListener = ActionListener.wrap(
            putTransformConfigurationResult -> {
                auditor.info(config.getId(), "Created data frame transform.");
                listener.onResponse(new AcknowledgedResponse(true));
            },
            listener::onFailure
        );

        // <4> Put our transform
        ActionListener<Boolean> pivotValidationListener = ActionListener.wrap(
            validationResult -> dataFrameTransformsConfigManager.putTransformConfiguration(config, putTransformConfigurationListener),
            validationException -> listener.onFailure(
                new RuntimeException(DataFrameMessages.REST_PUT_DATA_FRAME_FAILED_TO_VALIDATE_DATA_FRAME_CONFIGURATION,
                    validationException))
        );

        // <1> Validate our pivot
        pivot.validate(client, config.getSource(), pivotValidationListener);
    }
}
