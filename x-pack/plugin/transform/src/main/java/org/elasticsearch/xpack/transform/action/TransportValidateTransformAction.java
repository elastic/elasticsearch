/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.license.License;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction.Response;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.FunctionFactory;
import org.elasticsearch.xpack.transform.utils.SourceDestValidations;

import java.util.Map;
import java.util.Optional;

public class TransportValidateTransformAction extends HandledTransportAction<Request, Response> {

    private final Client client;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final boolean isRemoteClusterClientNode;
    private final SourceDestValidator sourceDestValidator;

    @Inject
    public TransportValidateTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        Settings settings,
        IngestService ingestService
    ) {
        this(
            ValidateTransformAction.NAME,
            transportService,
            actionFilters,
            client,
            indexNameExpressionResolver,
            clusterService,
            settings,
            ingestService);
    }

    protected TransportValidateTransformAction(
        String name,
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        Settings settings,
        IngestService ingestService
    ) {
        super(name, transportService, actionFilters, Request::new);
        this.client = client;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.isRemoteClusterClientNode = DiscoveryNode.isRemoteClusterClient(settings);
        this.sourceDestValidator = new SourceDestValidator(
            indexNameExpressionResolver,
            transportService.getRemoteClusterService(),
            isRemoteClusterClientNode
                /* transforms are BASIC so always allowed, no need to check license */
                ? new RemoteClusterLicenseChecker(client, mode -> true) : null,
            ingestService,
            clusterService.getNodeName(),
            License.OperationMode.BASIC.description()
        );
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState clusterState = clusterService.state();
        TransformNodes.warnIfNoTransformNodes(clusterState);
        final DiscoveryNodes nodes = clusterState.nodes();

        if (request.getConfig().getSource().requiresRemoteCluster() && (isRemoteClusterClientNode == false)) {
            // remote_cluster_client role is required but the current node is not remote_cluster_client, find another node.
            Optional<DiscoveryNode> remoteClusterClientNode = TransformNodes.selectAnyTransformRemoteNode(nodes);
            if (remoteClusterClientNode.isPresent()) {
                // Redirect the request to a remote_cluster_client node
                transportService.sendRequest(
                    remoteClusterClientNode.get(), actionName, request, new ActionListenerResponseHandler<>(listener, Response::new));
            } else {
                // There are no remote_cluster_client nodes in the cluster, fail
                listener.onFailure(ExceptionsHelper.badRequestException("No remote_cluster_client node to run on"));
            }
            return;
        }

        final TransformConfig config = request.getConfig();
        final Function function = FunctionFactory.create(config);

        // <5> Final listener
        ActionListener<Map<String, String>> deduceMappingsListener = ActionListener.wrap(
            mappings -> {
                listener.onResponse(new Response(mappings));
            },
            deduceTargetMappingsException -> listener.onFailure(
                new RuntimeException(
                    TransformMessages.REST_PUT_TRANSFORM_FAILED_TO_DEDUCE_DEST_MAPPINGS,
                    deduceTargetMappingsException)
            )
        );

        // <4> Deduce destination index mappings
        ActionListener<Boolean> validateQueryListener = ActionListener.wrap(
            validateQueryResponse -> {
                function.deduceMappings(client, config.getSource(), deduceMappingsListener);
            },
            listener::onFailure
        );

        // <3> Validate transform query
        ActionListener<Boolean> validateConfigListener = ActionListener.wrap(
            validateConfigResponse -> {
                if (request.isDeferValidation()) {
                    validateQueryListener.onResponse(true);
                } else {
                    function.validateQuery(client, config.getSource(), validateQueryListener);
                }
            },
            listener::onFailure
        );

        // <2> Validate transform function config
        ActionListener<Boolean> validateSourceDestListener = ActionListener.wrap(
            validateSourceDestResponse -> {
                function.validateConfig(validateConfigListener);
            },
            listener::onFailure
        );

        // <1> Validate source and destination indices
        sourceDestValidator.validate(
            clusterState,
            config.getSource().getIndex(),
            config.getDestination().getIndex(),
            config.getDestination().getPipeline(),
            SourceDestValidations.getValidations(request.isDeferValidation(), config.getAdditionalValidations()),
            validateSourceDestListener
        );
    }
}
