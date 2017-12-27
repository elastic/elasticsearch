/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.XpackField;
import org.elasticsearch.xpack.ml.MLMetadataField;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.security.SecurityContext;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesAction;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.security.action.user.HasPrivilegesResponse;
import org.elasticsearch.xpack.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.security.support.Exceptions;

import java.io.IOException;

public class TransportPutDatafeedAction extends TransportMasterNodeAction<PutDatafeedAction.Request, PutDatafeedAction.Response> {

    private final XPackLicenseState licenseState;
    private final Client client;
    private final boolean securityEnabled;
    private final SecurityContext securityContext;

    @Inject
    public TransportPutDatafeedAction(Settings settings, TransportService transportService,
                                      ClusterService clusterService, ThreadPool threadPool, Client client,
                                      XPackLicenseState licenseState, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, PutDatafeedAction.NAME, transportService, clusterService, threadPool,
                actionFilters, indexNameExpressionResolver, PutDatafeedAction.Request::new);
        this.licenseState = licenseState;
        this.client = client;
        this.securityEnabled = XPackSettings.SECURITY_ENABLED.get(settings);
        this.securityContext = securityEnabled ? new SecurityContext(settings, threadPool.getThreadContext()) : null;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected PutDatafeedAction.Response newResponse() {
        return new PutDatafeedAction.Response();
    }

    @Override
    protected void masterOperation(PutDatafeedAction.Request request, ClusterState state,
                                   ActionListener<PutDatafeedAction.Response> listener) {
        // If security is enabled only create the datafeed if the user requesting creation has
        // permission to read the indices the datafeed is going to read from
        if (securityEnabled) {
            final String username = securityContext.getUser().principal();
            ActionListener<HasPrivilegesResponse> privResponseListener = ActionListener.wrap(
                    r -> handlePrivsResponse(username, request, r, listener),
                    listener::onFailure);

            HasPrivilegesRequest privRequest = new HasPrivilegesRequest();
            privRequest.username(username);
            privRequest.clusterPrivileges(Strings.EMPTY_ARRAY);
            // We just check for permission to use the search action.  In reality we'll also
            // use the scroll action, but that's considered an implementation detail.
            privRequest.indexPrivileges(RoleDescriptor.IndicesPrivileges.builder()
                    .indices(request.getDatafeed().getIndices().toArray(new String[0]))
                    .privileges(SearchAction.NAME)
                    .build());

            client.execute(HasPrivilegesAction.INSTANCE, privRequest, privResponseListener);
        } else {
            putDatafeed(request, listener);
        }
    }

    private void handlePrivsResponse(String username, PutDatafeedAction.Request request,
                                     HasPrivilegesResponse response,
                                     ActionListener<PutDatafeedAction.Response> listener) throws IOException {
        if (response.isCompleteMatch()) {
            putDatafeed(request, listener);
        } else {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            for (HasPrivilegesResponse.IndexPrivileges index : response.getIndexPrivileges()) {
                builder.field(index.getIndex());
                builder.map(index.getPrivileges());
            }
            builder.endObject();

            listener.onFailure(Exceptions.authorizationError("Cannot create datafeed [{}]" +
                            " because user {} lacks permissions on the indices to be" +
                            " searched: {}",
                    request.getDatafeed().getId(), username, builder.string()));
        }
    }

    private void putDatafeed(PutDatafeedAction.Request request, ActionListener<PutDatafeedAction.Response> listener) {

        clusterService.submitStateUpdateTask(
                "put-datafeed-" + request.getDatafeed().getId(),
                new AckedClusterStateUpdateTask<PutDatafeedAction.Response>(request, listener) {

                    @Override
                    protected PutDatafeedAction.Response newResponse(boolean acknowledged) {
                        if (acknowledged) {
                            logger.info("Created datafeed [{}]", request.getDatafeed().getId());
                        }
                        return new PutDatafeedAction.Response(acknowledged, request.getDatafeed());
                    }

                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        return putDatafeed(request, currentState);
                    }
                });
    }

    private ClusterState putDatafeed(PutDatafeedAction.Request request, ClusterState clusterState) {
        MlMetadata currentMetadata = clusterState.getMetaData().custom(MLMetadataField.TYPE);
        MlMetadata newMetadata = new MlMetadata.Builder(currentMetadata)
                .putDatafeed(request.getDatafeed(), threadPool.getThreadContext()).build();
        return ClusterState.builder(clusterState).metaData(
                MetaData.builder(clusterState.getMetaData()).putCustom(MLMetadataField.TYPE, newMetadata).build())
                .build();
    }

    @Override
    protected ClusterBlockException checkBlock(PutDatafeedAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected void doExecute(Task task, PutDatafeedAction.Request request, ActionListener<PutDatafeedAction.Response> listener) {
        if (licenseState.isMachineLearningAllowed()) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XpackField.MACHINE_LEARNING));
        }
    }
}
