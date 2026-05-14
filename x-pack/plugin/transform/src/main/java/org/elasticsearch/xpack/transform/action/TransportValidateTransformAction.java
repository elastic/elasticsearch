/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.license.License;
import org.elasticsearch.license.RemoteClusterLicenseChecker;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatures;
import org.elasticsearch.xpack.core.common.validation.SourceDestValidator;
import org.elasticsearch.xpack.core.transform.TransformDeprecations;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction.Response;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.transforms.FunctionFactory;
import org.elasticsearch.xpack.transform.transforms.TransformNodes;
import org.elasticsearch.xpack.transform.utils.SourceDestValidations;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.core.Strings.format;

public class TransportValidateTransformAction extends HandledTransportAction<Request, Response> {
    private final Client client;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Settings nodeSettings;
    private final SourceDestValidator sourceDestValidator;
    private final CrossProjectModeDecider crossProjectModeDecider;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final FeatureService featureService;
    private final ProjectResolver projectResolver;

    @Inject
    public TransportValidateTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Client client,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        Settings settings,
        IngestService ingestService,
        TransformServices transformServices,
        FeatureService featureService,
        ProjectResolver projectResolver
    ) {
        super(ValidateTransformAction.NAME, transportService, actionFilters, Request::new, EsExecutors.DIRECT_EXECUTOR_SERVICE);
        this.client = client;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.nodeSettings = settings;
        this.sourceDestValidator = new SourceDestValidator(
            indexNameExpressionResolver,
            transportService.getRemoteClusterService(),
            DiscoveryNode.isRemoteClusterClient(settings)
                /* transforms are BASIC so always allowed, no need to check license */
                ? new RemoteClusterLicenseChecker(client, null)
                : null,
            ingestService,
            clusterService.getNodeName(),
            License.OperationMode.BASIC.description()
        );
        this.crossProjectModeDecider = transformServices.crossProjectModeDecider();
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.featureService = featureService;
        this.projectResolver = projectResolver;
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState clusterState = clusterService.state();

        // Mixed-cluster gate: reject [dest.op_type: create] unless every node supports the feature. Without this guard,
        // DestConfig.writeTo drops the op_type byte under the transport-version guard when talking to an older node,
        // silently reverting an opted-in append-only transform to INDEX semantics. We enforce this even when
        // defer_validation is set, since the transform may be started before the cluster is fully upgraded.
        if (request.getConfig().getDestination().getOpType() == DocWriteRequest.OpType.CREATE
            && featureService.clusterHasFeature(clusterState, XPackFeatures.TRANSFORM_DEST_OP_TYPE) == false) {
            listener.onFailure(
                new ValidationException().addValidationError(
                    "[dest.op_type] is [create], but the feature is not yet supported by all nodes in the cluster; "
                        + "please complete upgrades before creating a transform with [dest.op_type: create]."
                )
            );
            return;
        }

        if (request.isDeferValidation() == false) {
            TransformNodes.throwIfNoTransformNodes(clusterState);
            boolean requiresRemote = request.getConfig().getSource().requiresRemoteCluster();
            if (TransformNodes.redirectToAnotherNodeIfNeeded(
                clusterState,
                nodeSettings,
                requiresRemote,
                transportService,
                actionName,
                request,
                Response::new,
                listener
            )) {
                return;
            }
        }

        TransformNodes.warnIfNoTransformNodes(clusterState);

        var config = request.getConfig();
        var function = FunctionFactory.create(config);
        var parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        var parentClient = new ParentTaskAssigningClient(client, parentTaskId);

        if (config.getVersion() == null || config.getVersion().before(TransformDeprecations.MIN_TRANSFORM_VERSION)) {
            listener.onFailure(
                new ValidationException().addValidationError(
                    format(
                        "Transform configuration is too old [%s], use the upgrade API to fix your transform. "
                            + "Minimum required version is [%s]",
                        config.getVersion(),
                        TransformDeprecations.MIN_TRANSFORM_VERSION
                    )
                )
            );
            return;
        }

        // <6> Final listener
        ActionListener<Map<String, String>> deduceMappingsListener = ActionListener.wrap(deducedMappings -> {
            listener.onResponse(new Response(deducedMappings));
        },
            deduceTargetMappingsException -> listener.onFailure(
                new RuntimeException(TransformMessages.REST_PUT_TRANSFORM_FAILED_TO_DEDUCE_DEST_MAPPINGS, deduceTargetMappingsException)
            )
        );

        // <5> Deduce destination index mappings
        ActionListener<Boolean> validateQueryListener = ActionListener.wrap(validateQueryResponse -> {
            if (request.isDeferValidation()) {
                deduceMappingsListener.onResponse(emptyMap());
            } else {
                function.deduceMappings(parentClient, config.getHeaders(), config.getId(), config.getSource(), deduceMappingsListener);
            }
        }, listener::onFailure);

        // <4> Validate transform query
        ActionListener<Boolean> validateConfigListener = validateQueryListener.delegateFailureAndWrap((l, ignored) -> {
            if (request.isDeferValidation()) {
                l.onResponse(true);
            } else {
                function.validateQuery(parentClient, config.getHeaders(), config.getSource(), request.ackTimeout(), l);
            }
        });

        // <3.5> Validate that the destination index/data stream exists when op_type is create
        ActionListener<Boolean> validateOpTypeListener = validateConfigListener.delegateFailureAndWrap((l, ignored) -> {
            if (request.isDeferValidation() == false && config.getDestination().getOpType() == DocWriteRequest.OpType.CREATE) {
                String destinationIndex = config.getDestination().getIndex();
                String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(
                    clusterState,
                    IndicesOptions.lenientExpandOpen(),
                    destinationIndex
                );
                boolean destExists = concreteIndices.length > 0
                    || projectResolver.getProjectMetadata(clusterState).dataStreams().containsKey(destinationIndex);
                if (destExists == false) {
                    l.onFailure(
                        new ValidationException().addValidationError(
                            "Destination index ["
                                + destinationIndex
                                + "] does not exist. When [dest.op_type] is [create], the destination index must exist before"
                                + " starting the transform."
                        )
                    );
                    return;
                }
            }
            l.onResponse(true);
        });

        // <3> Validate Project Routing is not set when CPS is not supported
        ActionListener<Boolean> validateProjectRoutingListener = validateOpTypeListener.delegateFailureAndWrap((l, ignored) -> {
            if (config.getSource().getProjectRouting() == null || crossProjectModeDecider.crossProjectEnabled()) {
                l.onResponse(true);
            } else {
                l.onFailure(
                    new ValidationException().addValidationError(
                        "Cross-project calls are not supported, but project_routing was requested: "
                            + config.getSource().getProjectRouting()
                    )
                );
            }
        });

        // <2> Validate transform function config
        ActionListener<Boolean> validateSourceDestListener = validateProjectRoutingListener.delegateFailureAndWrap(
            (l, ignored) -> function.validateConfig(l)
        );

        // <1> Validate source and destination indices
        sourceDestValidator.validate(
            clusterState,
            config.getSource().getIndex(),
            config.getDestination().getIndex(),
            config.getDestination().getPipeline(),
            SourceDestValidations.getValidations(request.isDeferValidation(), config.getAdditionalSourceDestValidations()),
            validateSourceDestListener
        );
    }
}
