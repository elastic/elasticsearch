/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.TransformConfigAutoMigration;
import org.elasticsearch.xpack.transform.TransformExtensionHolder;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.AuthorizationStatePersistenceUtils;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;
import org.elasticsearch.xpack.transform.transforms.FunctionFactory;
import org.elasticsearch.xpack.transform.transforms.TransformNodes;

import java.time.Instant;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.xpack.transform.utils.SecondaryAuthorizationUtils.getSecurityHeadersPreferringSecondary;

public class TransportPutTransformAction extends AcknowledgedTransportMasterNodeAction<Request> {

    private static final Logger logger = LogManager.getLogger(TransportPutTransformAction.class);

    private final Settings settings;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final Client client;
    private final TransformConfigManager transformConfigManager;
    private final SecurityContext securityContext;
    private final TransformAuditor auditor;
    private final TransformConfigAutoMigration transformConfigAutoMigration;
    private final BooleanSupplier hasLinkedProjects;
    private final ProjectResolver projectResolver;
    private final TransformExtensionHolder extension;
    private final TransformCloudCredentialManager cloudCredentialManager;

    @Inject
    public TransportPutTransformAction(
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        TransformServices transformServices,
        Client client,
        TransformConfigAutoMigration transformConfigAutoMigration,
        ProjectResolver projectResolver,
        TransformExtensionHolder extension
    ) {
        super(
            PutTransformAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutTransformAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.settings = settings;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.client = client;
        this.transformConfigManager = transformServices.configManager();
        this.extension = extension;
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.auditor = transformServices.auditor();
        this.transformConfigAutoMigration = transformConfigAutoMigration;
        this.hasLinkedProjects = () -> transformServices.hasLinkedProjects().apply(projectResolver.getProjectId());
        this.projectResolver = projectResolver;
        this.cloudCredentialManager = transformServices.cloudCredentialManager();
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<AcknowledgedResponse> listener) {
        // Extract on the coordinating node, before the request is forwarded to master — the
        // AUTHENTICATING_CLOUD_TOKEN_THREAD_CONTEXT transient does not survive master forwarding.
        CloudCredential callerCredential = cloudCredentialManager.currentCallerCredential();
        if (callerCredential != null) {
            request.setCloudCredential(callerCredential);
        }
        super.doExecute(task, request, ActionListener.releaseAfter(listener, request));
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState clusterState, ActionListener<AcknowledgedResponse> listener) {
        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);
        if (request.isDeferValidation() == false && TransformNodes.hasNoTransformNodes(clusterState)) {
            TransformNodes.completeWithNoTransformNodeException(listener);
            return;
        }
        TransformNodes.warnIfNoTransformNodes(projectResolver.getProjectMetadata(clusterState), clusterState.getNodes());

        if (TransformMetadata.isUpgradeMode(projectResolver.getProjectMetadata(clusterState))) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot create new Transform while the Transform feature is upgrading.",
                    RestStatus.CONFLICT
                )
            );
            return;
        }

        TransformConfig config = request.getConfig().setCreateTime(Instant.now()).setVersion(TransformConfigVersion.CURRENT);
        config.setHeaders(getSecurityHeadersPreferringSecondary(threadPool, securityContext, clusterState));

        String transformId = config.getId();
        // quick check whether a transform has already been created under that name
        if (PersistentTasksCustomMetadata.getTaskWithId(clusterState, transformId) != null) {
            listener.onFailure(
                new ResourceAlreadyExistsException(TransformMessages.getMessage(TransformMessages.REST_PUT_TRANSFORM_EXISTS, transformId))
            );
            return;
        }

        // <5> Create the transform, stamping the minted tokenId (if any) onto the config so the
        // running task and indexer can later load the credential by id.
        ActionListener<String> mintCredentialListener = listener.delegateFailureAndWrap(
            (l, mintedTokenId) -> putTransform(config.withCredentialId(mintedTokenId), mintedTokenId, l)
        );

        // <4> Mint cloud credential if UIAM is present (no-op when the feature is off: mintAndPersist
        // sees no caller credential and responds with a null tokenId). Passes the request's own
        // credential directly (no copy needed): mint runs after <3> below, which already dispatched
        // and closed its own independent copy, so nothing else still needs this reference. The
        // outer doExecute-level releaseAfter(listener, request) closing the same instance again once
        // the whole PUT resolves is a safe, idempotent no-op.
        ActionListener<ValidateTransformAction.Response> validateTransformListener = mintCredentialListener
            .delegateFailureIgnoreResponseAndWrap(l -> cloudCredentialManager.mintAndPersist(transformId, request.getCloudCredential(), l));

        // <3> Validate source and destination indices
        var parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        var parentTaskClient = new ParentTaskAssigningClient(client, parentTaskId);
        ActionListener<Void> checkPrivilegesListener = validateTransformListener.delegateFailureAndWrap((l, aVoid) -> {
            // Hoist into a local so we can hand the same instance to executeAsyncWithOrigin and to
            // releaseAfter, which closes the request (and its CloudCredential SecureString) once the
            // dispatch listener fires. Plugs the leak when the request is forwarded to a remote node
            // or when dispatch fails synchronously before the receiver-side releaseAfter is set up.
            // Request.close() is null-safe so this path is identical for non-UIAM callers.
            //
            // Uses an independent copy of the credential: TransportValidateTransformAction
            // unconditionally closes whatever credential this request carries once validate resolves
            // (it has to, to cover the redirect-to-another-node case), which would zero out the
            // request's own credential before <4> above gets to mint with it.
            var validateRequest = new ValidateTransformAction.Request(
                config,
                request.isDeferValidation(),
                request.ackTimeout(),
                CloudCredential.copyOf(request.getCloudCredential())
            );
            ClientHelper.executeAsyncWithOrigin(
                parentTaskClient,
                ClientHelper.TRANSFORM_ORIGIN,
                ValidateTransformAction.INSTANCE,
                true,
                validateRequest,
                ActionListener.releaseAfter(l, validateRequest)
            );
        });

        // <2> Early check to verify that the user can create the destination index and can read from the source
        ActionListener<Void> createIndexListener = checkPrivilegesListener.delegateFailureAndWrap((l, r) -> {
            if (XPackSettings.SECURITY_ENABLED.get(settings)) {
                TransformPrivilegeChecker.checkPrivileges(
                    "create",
                    settings,
                    securityContext,
                    indexNameExpressionResolver,
                    clusterState,
                    client,
                    config,
                    true,
                    hasLinkedProjects.getAsBoolean(),
                    ActionListener.wrap(
                        aVoid -> AuthorizationStatePersistenceUtils.persistAuthState(
                            settings,
                            transformConfigManager,
                            transformId,
                            AuthorizationState.green(),
                            l
                        ),
                        e -> {
                            if (request.isDeferValidation()) {
                                AuthorizationStatePersistenceUtils.persistAuthState(
                                    settings,
                                    transformConfigManager,
                                    transformId,
                                    AuthorizationState.red(e),
                                    l
                                );
                            } else {
                                l.onFailure(e);
                            }
                        }
                    )
                );
            } else { // No security enabled, just move on
                l.onResponse(null);
            }
        });

        // <1> Check the latest internal index (IMPORTANT: according to _this_ node, which might be newer than master) is installed
        TransformInternalIndex.createLatestVersionedIndexIfRequired(
            clusterService,
            new ParentTaskAssigningClient(client, parentTaskId),
            projectResolver.getProjectId(),
            extension.getTransformExtension().getTransformInternalIndexAdditionalSettings(),
            createIndexListener.delegateResponse((l, e) -> {
                l.onFailure(
                    new ElasticsearchStatusException(
                        TransformMessages.REST_PUT_FAILED_CREATING_TRANSFORM_INDEX,
                        RestStatus.SERVICE_UNAVAILABLE,
                        ExceptionsHelper.unwrapCause(e)
                    )
                );
            })
        );
    }

    @Override
    protected ClusterBlockException checkBlock(PutTransformAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(projectResolver.getProjectId(), ClusterBlockLevel.METADATA_WRITE);
    }

    private void putTransform(
        TransformConfig originalConfig,
        @Nullable String mintedTokenId,
        ActionListener<AcknowledgedResponse> listener
    ) {
        var config = transformConfigAutoMigration.migrate(originalConfig);
        var transformId = config.getId();
        transformConfigManager.putTransformConfiguration(config, ActionListener.wrap(unused -> {
            logger.info("[{}] created transform", transformId);
            auditor.info(transformId, "Created transform.");

            var validationFunc = FunctionFactory.create(config);
            TransformConfigLinter.getWarnings(validationFunc, config.getSource(), config.getSyncConfig()).forEach(warning -> {
                logger.warn("[{}] {}", transformId, warning);
                auditor.warning(transformId, warning);
            });

            listener.onResponse(AcknowledgedResponse.TRUE);
        }, configWriteFailure -> {
            // Roll back the just-minted credential so we never leave an orphan at UIAM nor an
            // empty/leaked storage doc. mintedTokenId is null when no UIAM context was present (the
            // helper short-circuits in that case).
            logger.debug("[{}] config write failed after credential mint [{}], compensating revoke + delete", transformId, mintedTokenId);
            cloudCredentialManager.loadRevokeAndDeleteByTokenId(
                transformId,
                mintedTokenId,
                ActionListener.running(() -> listener.onFailure(configWriteFailure))
            );
        }));
    }
}
