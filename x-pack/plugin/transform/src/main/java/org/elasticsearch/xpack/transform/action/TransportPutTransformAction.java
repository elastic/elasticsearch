/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction;
import org.elasticsearch.xpack.core.transform.action.PutTransformAction.Request;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.Function;
import org.elasticsearch.xpack.transform.transforms.FunctionFactory;

import java.time.Instant;
import java.util.List;
import java.util.Map;

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
        Client client,
        IngestService ingestService
    ) {
        this(
            PutTransformAction.NAME,
            settings,
            transportService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            clusterService,
            transformServices,
            client,
            ingestService
        );
    }

    protected TransportPutTransformAction(
        String name,
        Settings settings,
        TransportService transportService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        TransformServices transformServices,
        Client client,
        IngestService ingestService
    ) {
        super(
            name,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            PutTransformAction.Request::new,
            indexNameExpressionResolver,
            ThreadPool.Names.SAME
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

        // set headers to run transform as calling user
        Map<String, String> filteredHeaders = ClientHelper.filterSecurityHeaders(threadPool.getThreadContext().getHeaders());

        TransformConfig config = request.getConfig().setHeaders(filteredHeaders).setCreateTime(Instant.now()).setVersion(Version.CURRENT);

        String transformId = config.getId();
        // quick check whether a transform has already been created under that name
        if (PersistentTasksCustomMetadata.getTaskWithId(clusterState, transformId) != null) {
            listener.onFailure(
                new ResourceAlreadyExistsException(TransformMessages.getMessage(TransformMessages.REST_PUT_TRANSFORM_EXISTS, transformId))
            );
            return;
        }

        // <3> Create the transform
        ActionListener<ValidateTransformAction.Response> validateTransformListener = ActionListener.wrap(
            validationResponse -> {
                putTransform(request, listener);
            },
            listener::onFailure
        );

        // <2> Validate source and destination indices
        ActionListener<Void> checkPrivilegesListener = ActionListener.wrap(
            aVoid -> {
                client.execute(
                    ValidateTransformAction.INSTANCE,
                    new ValidateTransformAction.Request(config, request.isDeferValidation()),
                    validateTransformListener
                );
            },
            listener::onFailure
        );

        // <1> Early check to verify that the user can create the destination index and can read from the source
        if (XPackSettings.SECURITY_ENABLED.get(settings) && request.isDeferValidation() == false) {
            TransformPrivilegeChecker.checkPrivileges(
                "create", securityContext, indexNameExpressionResolver, clusterState, client, config, true, checkPrivilegesListener);
        } else { // No security enabled, just move on
            checkPrivilegesListener.onResponse(null);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutTransformAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void putTransform(Request request, ActionListener<AcknowledgedResponse> listener) {

        final TransformConfig config = request.getConfig();
        // create the function for validation
        final Function function = FunctionFactory.create(config);

        // <2> Return to the listener
        ActionListener<Boolean> putTransformConfigurationListener = ActionListener.wrap(putTransformConfigurationResult -> {
            logger.debug("[{}] created transform", config.getId());
            auditor.info(config.getId(), "Created transform.");
            List<String> warnings = TransformConfigLinter.getWarnings(function, config.getSource(), config.getSyncConfig());
            for (String warning : warnings) {
                logger.warn(new ParameterizedMessage("[{}] {}", config.getId(), warning));
                auditor.warning(config.getId(), warning);
            }
            listener.onResponse(AcknowledgedResponse.TRUE);
        }, listener::onFailure);

        // <1> Put our transform
        transformConfigManager.putTransformConfiguration(config, putTransformConfigurationListener);
    }
}
