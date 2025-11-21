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

import java.util.Objects;

public class CCMService {

    private static final Logger logger = LogManager.getLogger(CCMService.class);

    private final CCMPersistentStorageService ccmPersistentStorageService;
    private final Client client;
    private final CCMEnablementService ccmEnablementService;
    private final ProjectResolver projectResolver;

    public CCMService(
        CCMPersistentStorageService ccmPersistentStorageService,
        CCMEnablementService enablementService,
        Client client,
        ProjectResolver projectResolver
    ) {
        this.ccmPersistentStorageService = Objects.requireNonNull(ccmPersistentStorageService);
        this.client = new OriginSettingClient(Objects.requireNonNull(client), ClientHelper.INFERENCE_ORIGIN);
        this.ccmEnablementService = Objects.requireNonNull(enablementService);
        this.projectResolver = Objects.requireNonNull(projectResolver);
        // TODO initialize the cache for the CCM configuration
    }

    public void isEnabled(ActionListener<Boolean> listener) {
        listener.onResponse(ccmEnablementService.isEnabled(projectResolver.getProjectId()));
    }

    public void storeConfiguration(CCMModel model, ActionListener<Void> listener) {
        SubscribableListener.<Void>newForked(storeListener -> ccmPersistentStorageService.store(model, storeListener))
            .<Void>andThen(
                enableAuthExecutorListener -> ccmEnablementService.setEnabled(
                    projectResolver.getProjectId(),
                    true,
                    ActionListener.wrap(ack -> {
                        logger.debug("Successfully set CCM enabled in enablement service");
                        enableAuthExecutorListener.onResponse(null);
                    }, e -> {
                        logger.atDebug().withThrowable(e).log("Failed to enable CCM in enablement service");
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
            disableAuthExecutorListener -> ccmEnablementService.setEnabled(
                projectResolver.getProjectId(),
                false,
                ActionListener.wrap(ack -> {
                    logger.debug("Successfully set CCM disabled in enablement service");
                    disableAuthExecutorListener.onResponse(null);
                }, e -> {
                    logger.atDebug().withThrowable(e).log("Failed to disable CCM in enablement service");
                    disableAuthExecutorListener.onFailure(e);
                })
            )
        ).andThen(ccmPersistentStorageService::delete).addListener(listener);

        // TODO implement invalidating the cache
    }
}
