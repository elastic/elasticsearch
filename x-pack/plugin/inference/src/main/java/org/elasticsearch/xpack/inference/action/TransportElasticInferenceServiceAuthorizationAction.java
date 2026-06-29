/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.MinimalServiceSettings;
import org.elasticsearch.inference.Model;
import org.elasticsearch.inference.metadata.EndpointMetadata;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.inference.action.AuthorizationAction;
import org.elasticsearch.xpack.core.inference.action.StoreInferenceEndpointsAction;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.features.InferenceFeatureService;
import org.elasticsearch.xpack.inference.registry.ModelRegistry;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.authorization.CcmDisabledException;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationModel;
import org.elasticsearch.xpack.inference.services.elastic.authorization.ElasticInferenceServiceAuthorizationRequestHandler;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMFeature;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMService;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Transport action that encapsulates the EIS authorization decision and the full auth/store pipeline.
 * <p>
 * {@code doExecute} replicates the logic formerly in {@code AuthorizationPoller.shouldSendAuthRequest}:
 * early-exit with an empty response when the registry is not ready or a required cluster feature is
 * missing; skip the send and signal the poller to complete via {@link CcmDisabledException} when CCM
 * is supported but disabled; otherwise call EIS, reconcile endpoints, and return the store result.
 */
public class TransportElasticInferenceServiceAuthorizationAction extends HandledTransportAction<
    AuthorizationAction.Request,
    StoreInferenceEndpointsAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportElasticInferenceServiceAuthorizationAction.class);

    static final StoreInferenceEndpointsAction.Response EMPTY_RESPONSE = new StoreInferenceEndpointsAction.Response(List.of());

    private final ModelRegistry modelRegistry;
    private final ElasticInferenceServiceAuthorizationRequestHandler authorizationHandler;
    private final Sender sender;
    private final CCMFeature ccmFeature;
    private final CCMService ccmService;
    private final InferenceFeatureService inferenceFeatureService;
    private final Client client;

    @Inject
    public TransportElasticInferenceServiceAuthorizationAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ModelRegistry modelRegistry,
        ElasticInferenceServiceAuthorizationRequestHandler authorizationHandler,
        Sender sender,
        CCMFeature ccmFeature,
        CCMService ccmService,
        InferenceFeatureService inferenceFeatureService,
        Client client
    ) {
        super(
            AuthorizationAction.NAME,
            transportService,
            actionFilters,
            AuthorizationAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.modelRegistry = Objects.requireNonNull(modelRegistry);
        this.authorizationHandler = Objects.requireNonNull(authorizationHandler);
        this.sender = Objects.requireNonNull(sender);
        this.ccmFeature = Objects.requireNonNull(ccmFeature);
        this.ccmService = Objects.requireNonNull(ccmService);
        this.inferenceFeatureService = Objects.requireNonNull(inferenceFeatureService);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ClientHelper.INFERENCE_ORIGIN);
    }

    @Override
    protected void doExecute(
        Task task,
        AuthorizationAction.Request request,
        ActionListener<StoreInferenceEndpointsAction.Response> listener
    ) {
        if (modelRegistry.isReady() == false) {
            logger.info("Skipping sending authorization request, because the model registry is not ready");
            listener.onResponse(EMPTY_RESPONSE);
            return;
        }

        if (inferenceFeatureService.hasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD) == false) {
            logger.info("Skipping sending authorization request, because the cluster is currently upgrading and missing required features");
            listener.onResponse(EMPTY_RESPONSE);
            return;
        }

        if (ccmFeature.isCcmSupportedEnvironment() == false) {
            sendRequest(listener);
            return;
        }

        ccmService.isEnabled(listener.delegateFailureAndWrap((delegate, enabled) -> {
            if (enabled == null || enabled == false) {
                delegate.onFailure(new CcmDisabledException());
                return;
            }
            sendRequest(delegate);
        }));
    }

    private void sendRequest(ActionListener<StoreInferenceEndpointsAction.Response> listener) {
        SubscribableListener.<ElasticInferenceServiceAuthorizationModel>newForked(
            authModelListener -> authorizationHandler.getAuthorization(authModelListener, sender)
        )
            .<ElasticInferenceServiceAuthorizationModel>andThen(
                (nextListener, authModel) -> deleteRemovedEndpoints(authModel, nextListener)
            )
            .andThenApply(this::selectEndpointsToPersist).<StoreInferenceEndpointsAction.Response>andThen(
                (storeListener, inferenceIdsToPersist) -> storePreconfiguredModels(inferenceIdsToPersist, storeListener)
            )
            .addListener(listener);
    }

    private void deleteRemovedEndpoints(
        ElasticInferenceServiceAuthorizationModel authModel,
        ActionListener<ElasticInferenceServiceAuthorizationModel> listener
    ) {
        var toDelete = new HashSet<>(authModel.getRemovedEndpoints());
        toDelete.retainAll(modelRegistry.getInferenceIds());

        if (toDelete.isEmpty()) {
            listener.onResponse(authModel);
            return;
        }

        logger.info("Deleting removed EIS inference endpoints: {}", toDelete);
        modelRegistry.deleteModels(toDelete, ActionListener.wrap(success -> listener.onResponse(authModel), e -> {
            logger.atWarn().withThrowable(e).log("Failed to delete removed EIS inference endpoints: {}", toDelete);
            listener.onResponse(authModel);
        }));
    }

    private List<Model> selectEndpointsToPersist(ElasticInferenceServiceAuthorizationModel authModel) {
        logger.debug("Received authorization response, {}", authModel);

        var scopedAuthModel = authModel.newLimitedToTaskTypes(EnumSet.copyOf(ElasticInferenceService.IMPLEMENTED_TASK_TYPES));
        logger.debug("Authorization entity limited to service task types, {}", scopedAuthModel);

        List<Model> endpoints = scopedAuthModel.getEndpoints(scopedAuthModel.getEndpointIds());

        // We get all existing endpoints from the registry in a single call to ensure all decisions
        // of a single authorization request are based on a single cluster state.
        Map<String, MinimalServiceSettings> existingById = modelRegistry.getMinimalServiceSettings(
            endpoints.stream().map(Model::getInferenceEntityId).collect(Collectors.toSet()),
            false
        );
        return endpoints.stream()
            .filter(model -> shouldPersistEndpoint(model, existingById.get(model.getInferenceEntityId())))
            .collect(Collectors.toList());
    }

    private static boolean shouldPersistEndpoint(Model newEndpoint, @Nullable MinimalServiceSettings existingEndpoint) {
        if (existingEndpoint == null) {
            logger.debug(
                () -> Strings.format(
                    "[%s] selected for persistence, because it currently does not exist",
                    newEndpoint.getInferenceEntityId()
                )
            );
            return true;
        }

        EndpointMetadata existingMetadata = existingEndpoint.endpointMetadata();
        if (existingMetadata.fingerprintMatches(newEndpoint.getConfigurations().getEndpointMetadataOrEmpty()) == false) {
            logger.debug(
                () -> Strings.format(
                    "[%s] selected for persistence, because its fingerprint has changed",
                    newEndpoint.getInferenceEntityId()
                )
            );
            return true;
        }
        if (newEndpoint.getConfigurations().getEndpointMetadataOrEmpty().hasNewerVersionThan(existingMetadata)) {
            logger.debug(
                () -> Strings.format("[%s] selected for persistence, because its version is higher", newEndpoint.getInferenceEntityId())
            );
            return true;
        }
        return false;
    }

    private void storePreconfiguredModels(List<Model> newEndpoints, ActionListener<StoreInferenceEndpointsAction.Response> listener) {
        if (newEndpoints.isEmpty()) {
            listener.onResponse(EMPTY_RESPONSE);
            return;
        }

        logger.info(
            "Storing EIS preconfigured inference endpoints with inference IDs {}",
            newEndpoints.stream().map(Model::getInferenceEntityId).toList()
        );
        var storeRequest = new StoreInferenceEndpointsAction.Request(newEndpoints, TimeValue.THIRTY_SECONDS);

        client.execute(StoreInferenceEndpointsAction.INSTANCE, storeRequest, listener.delegateFailureAndWrap((d, responses) -> {
            for (var response : responses.getResults()) {
                if (response.failed()) {
                    logger.atWarn()
                        .withThrowable(response.failureCause())
                        .log("Failed to store new EIS preconfigured inference endpoint with inference ID [{}]", response.inferenceId());
                } else {
                    logger.atInfo()
                        .log("Successfully stored EIS preconfigured inference endpoint with inference ID [{}]", response.inferenceId());
                }
            }
            d.onResponse(responses);
        }));
    }
}
