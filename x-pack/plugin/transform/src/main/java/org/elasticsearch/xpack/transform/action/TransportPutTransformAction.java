/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.AuthorizationStatePersistenceUtils;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.FunctionFactory;

import java.time.Instant;

import static org.elasticsearch.xpack.transform.utils.SecondaryAuthorizationUtils.getSecurityHeadersPreferringSecondary;

public class TransportPutTransformAction extends AcknowledgedTransportMasterNodeAction<Request> {

    private static final Logger logger = LogManager.getLogger(TransportPutTransformAction.class);

    private final Settings settings;
    private final Client client;
    private final TransformConfigManager transformConfigManager;
    private final SecurityContext securityContext;
    private final TransformAuditor auditor;

    @Inject
    public TransportPutTransformAction(
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        TransformServices transformServices,
        Client client
    ) {
        super(
            PutTransformAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutTransformAction.Request::new,
            indexNameExpressionResolver,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.settings = settings;
        this.client = client;
        this.transformConfigManager = transformServices.getConfigManager();
        this.securityContext = XPackSettings.SECURITY_ENABLED.get(settings)
            ? new SecurityContext(settings, threadPool.getThreadContext())
            : null;
        this.auditor = transformServices.getAuditor();
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState clusterState, ActionListener<AcknowledgedResponse> listener) {
        XPackPlugin.checkReadyForXPackCustomMetadata(clusterState);

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

        // <3> Create the transform
        ActionListener<ValidateTransformAction.Response> validateTransformListener = listener.delegateFailureAndWrap(
            (l, unused) -> putTransform(request, l)
        );

        // <2> Validate source and destination indices
        ActionListener<Void> checkPrivilegesListener = validateTransformListener.delegateFailureAndWrap(
            (l, aVoid) -> ClientHelper.executeAsyncWithOrigin(
                client,
                ClientHelper.TRANSFORM_ORIGIN,
                ValidateTransformAction.INSTANCE,
                new ValidateTransformAction.Request(config, request.isDeferValidation(), request.timeout()),
                l
            )
        );

        // <1> Early check to verify that the user can create the destination index and can read from the source
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
                        checkPrivilegesListener
                    ),
                    e -> {
                        if (request.isDeferValidation()) {
                            AuthorizationStatePersistenceUtils.persistAuthState(
                                settings,
                                transformConfigManager,
                                transformId,
                                AuthorizationState.red(e),
                                checkPrivilegesListener
                            );
                        } else {
                            checkPrivilegesListener.onFailure(e);
                        }
                    }
                )
            );
        } else { // No security enabled, just move on
            checkPrivilegesListener.onResponse(null);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutTransformAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void putTransform(Request request, ActionListener<AcknowledgedResponse> listener) {
        var config = request.getConfig();
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
