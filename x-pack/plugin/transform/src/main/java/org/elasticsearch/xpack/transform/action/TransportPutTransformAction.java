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
    private final ProjectResolver projectResolver;
    private final TransformExtensionHolder extension;

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
        this.projectResolver = projectResolver;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState clusterState, ActionListener<AcknowledgedResponse> listener) {
        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);
        if (request.isDeferValidation() == false && TransformNodes.hasNoTransformNodes(clusterState)) {
            TransformNodes.completeWithNoTransformNodeException(listener);
            return;
        }
        TransformNodes.warnIfNoTransformNodes(clusterState);

        if (TransformMetadata.upgradeMode(clusterState)) {
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

        // <4> Create the transform
        ActionListener<ValidateTransformAction.Response> validateTransformListener = listener.delegateFailureAndWrap(
            (l, unused) -> putTransform(request, l)
        );

        // <3> Validate source and destination indices

        var parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        ActionListener<Void> checkPrivilegesListener = validateTransformListener.delegateFailureAndWrap(
            (l, aVoid) -> ClientHelper.executeAsyncWithOrigin(
                new ParentTaskAssigningClient(client, parentTaskId),
                ClientHelper.TRANSFORM_ORIGIN,
                ValidateTransformAction.INSTANCE,
                new ValidateTransformAction.Request(config, request.isDeferValidation(), request.ackTimeout()),
                l
            )
        );

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
            extension.getTransformExtension().getTransformInternalIndexAdditionalSettings(),
            createIndexListener.delegateResponse((l, e) -> {
                l.onFailure(
                    new ElasticsearchStatusException(
                        TransformMessages.REST_PUT_FAILED_CREATING_TRANSFORM_INDEX,
                        RestStatus.INTERNAL_SERVER_ERROR,
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

    private void putTransform(Request request, ActionListener<AcknowledgedResponse> listener) {
        var config = transformConfigAutoMigration.migrate(request.getConfig());
        transformConfigManager.putTransformConfiguration(config, listener.delegateFailureAndWrap((l, unused) -> {
            var transformId = config.getId();
            logger.info("[{}] created transform", transformId);
            auditor.info(transformId, "Created transform.");

            var validationFunc = FunctionFactory.create(config);
            TransformConfigLinter.getWarnings(validationFunc, config.getSource(), config.getSyncConfig()).forEach(warning -> {
                logger.warn("[{}] {}", transformId, warning);
                auditor.warning(transformId, warning);
            });

            l.onResponse(AcknowledgedResponse.TRUE);
        }));
    }
}
