/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.ccm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.inference.services.elastic.authorization.AuthorizationTaskExecutor;

import java.util.Objects;

public class CCMService {

    private static final Logger logger = LogManager.getLogger(CCMService.class);

    private final CCMPersistentStorageService ccmPersistentStorageService;
    private final Client client;

    public CCMService(CCMPersistentStorageService ccmPersistentStorageService, Client client) {
        this.ccmPersistentStorageService = Objects.requireNonNull(ccmPersistentStorageService);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ClientHelper.INFERENCE_ORIGIN);
        // TODO initialize the cache for the CCM configuration
    }

    public void isEnabled(ActionListener<Boolean> listener) {
        // TODO use cache or cluster state to determine if CCM is enabled
        var ccmModelListener = ActionListener.<CCMModel>wrap(ignored -> listener.onResponse(true), e -> {
            if (e instanceof ResourceNotFoundException) {
                listener.onResponse(false);
                return;
            }

            listener.onFailure(e);
        });

        ccmPersistentStorageService.get(ccmModelListener);
    }

    public void storeConfiguration(CCMModel model, ActionListener<Void> listener) {
        SubscribableListener.<Void>newForked(storeListener -> ccmPersistentStorageService.store(model, storeListener))
            .<Void>andThen(
                enableAuthExecutorListener -> client.execute(
                    AuthorizationTaskExecutor.Action.INSTANCE,
                    AuthorizationTaskExecutor.Action.request(AuthorizationTaskExecutor.Message.ENABLE_MESSAGE, null),
                    ActionListener.wrap(ack -> {
                        logger.debug("Successfully enabled authorization task executor");
                        enableAuthExecutorListener.onResponse(null);
                    }, e -> {
                        logger.atDebug().withThrowable(e).log("Failed to enable authorization task executor");
                        enableAuthExecutorListener.onFailure(e);
                    })
                )
            )
            .addListener(listener);

        // TODO invalidate the cache
    }

    public void getConfiguration(ActionListener<CCMModel> listener) {
        // TODO get this from the cache instead
        ccmPersistentStorageService.get(listener);
    }

    public void disableCCM(ActionListener<Void> listener) {
        SubscribableListener.<Void>newForked(
            disableAuthExecutorListener -> client.execute(
                AuthorizationTaskExecutor.Action.INSTANCE,
                AuthorizationTaskExecutor.Action.request(AuthorizationTaskExecutor.Message.DISABLE_MESSAGE, null),
                ActionListener.wrap(ack -> {
                    logger.debug("Successfully disabled authorization task executor");
                    disableAuthExecutorListener.onResponse(null);
                }, e -> {
                    logger.atDebug().withThrowable(e).log("Failed to disable authorization task executor");
                    disableAuthExecutorListener.onFailure(e);
                })
            )
        ).andThen(ccmPersistentStorageService::delete).addListener(listener);

        // TODO implement invalidating the cache
    }
}
