/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationTaskExecutor;

import java.util.Objects;

public class CCMService {

    private static final Logger logger = LogManager.getLogger(CCMService.class);

    private final CCMPersistentStorageService ccmPersistentStorageService;
    private final CCMEnablementService ccmEnablementService;
    private final CCMCache ccmCache;
    private final ProjectResolver projectResolver;
    private final Client client;

    public CCMService(
        CCMPersistentStorageService ccmPersistentStorageService,
        CCMEnablementService enablementService,
        CCMCache ccmCache,
        ProjectResolver projectResolver,
        Client client
    ) {
        this.ccmPersistentStorageService = Objects.requireNonNull(ccmPersistentStorageService);
        this.ccmEnablementService = Objects.requireNonNull(enablementService);
        this.ccmCache = Objects.requireNonNull(ccmCache);
        this.projectResolver = Objects.requireNonNull(projectResolver);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ClientHelper.INFERENCE_ORIGIN);
    }

    public void isEnabled(ActionListener<Boolean> listener) {
        listener.onResponse(ccmEnablementService.isEnabled(projectResolver.getProjectId()));
    }

    public void storeConfiguration(CCMModel model, ActionListener<Void> listener) {
        SubscribableListener.<Void>newForked(storeListener -> ccmPersistentStorageService.store(model, storeListener))
            .<Void>andThen(
                enablementListener -> ccmEnablementService.setEnabled(projectResolver.getProjectId(), true, ActionListener.wrap(ack -> {
                    logger.debug("Successfully set CCM enabled in enablement service");
                    enablementListener.onResponse(null);
                }, e -> {
                    logger.atWarn().withThrowable(e).log("Failed to enable CCM in enablement service");
                    enablementListener.onFailure(e);
                }))
            )
            .<Void>andThen(
                // if we fail to invalidate the cache, it's not a big deal since the cache will eventually expire
                invalidateCacheListener -> ccmCache.invalidate(
                    invalidateCacheListener.delegateResponse((delegate, e) -> delegate.onResponse(null))
                )
            )
            .<Void>andThen(
                enableAuthExecutorListener -> client.execute(
                    AuthorizationTaskExecutor.Action.INSTANCE,
                    AuthorizationTaskExecutor.Action.request(AuthorizationTaskExecutor.Message.ENABLE_MESSAGE, null),
                    ActionListener.wrap(ack -> {
                        logger.debug("Successfully enabled authorization task executor");
                        enableAuthExecutorListener.onResponse(null);
                    }, e -> {
                        logger.atWarn().withThrowable(e).log("Failed to request start of CCM authorization task");
                        // even if requesting start of the authorization task fails, we still consider CCM enabled because
                        // the cluster state listener will eventually start the task if it is missing
                        enableAuthExecutorListener.onResponse(null);
                    })
                )
            )
            .addListener(listener);
    }

    public void getConfiguration(ActionListener<CCMModel> listener) {
        ccmCache.get(listener);
    }

    public void disableCCM(ActionListener<Void> listener) {
        SubscribableListener.<Void>newForked(
            disableAuthExecutorListener -> ccmEnablementService.setEnabled(
                projectResolver.getProjectId(),
                false,
                ActionListener.wrap(ack -> {
                    logger.debug("Successfully set CCM disabled in enablement service");
                    disableAuthExecutorListener.onResponse(null);
                }, e -> {
                    logger.atWarn().withThrowable(e).log("Failed to disable CCM in enablement service");
                    disableAuthExecutorListener.onFailure(e);
                })
            )
        )
            .andThen(ccmPersistentStorageService::delete)
            // if we fail to invalidate the cache, it's not a big deal since the cache will eventually expire
            .<Void>andThen(
                invalidateCacheListener -> ccmCache.invalidate(
                    invalidateCacheListener.delegateResponse((delegate, e) -> delegate.onResponse(null))
                )
            )
            .<Void>andThen(
                disableAuthExecutorListener -> client.execute(
                    AuthorizationTaskExecutor.Action.INSTANCE,
                    AuthorizationTaskExecutor.Action.request(AuthorizationTaskExecutor.Message.DISABLE_MESSAGE, null),
                    ActionListener.wrap(ack -> {
                        logger.debug("Successfully disabled CCM authorization task executor");
                        disableAuthExecutorListener.onResponse(null);
                    }, e -> {
                        logger.atWarn().withThrowable(e).log("Stop request for CCM authorization task failed");
                        // even if stopping the authorization task fails, we still consider CCM disabled because the tasks themselves
                        // should eventually stop after they check if ccm is enabled
                        disableAuthExecutorListener.onResponse(null);
                    })
                )
            )
            .addListener(listener);
    }
}
