/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.telemetry.TransformMeterRegistry;

/**
 * Intended to automatically apply configuration changes as part of `PUT _transform` or during the persistent task's start
 * in {@link org.elasticsearch.xpack.transform.transforms.TransformPersistentTasksExecutor}.
 *
 * This is intentionally separate from {@link org.elasticsearch.xpack.transform.action.TransformUpdater}, which is designed to be called
 * from `_update` and `_upgrade` and apply potentially breaking changes and a broader set of changes.
 * This class is more designed for serverless rolling updates to apply a smaller non-breaking subset of changes.
 */
public class TransformConfigAutoMigration {

    private static final Logger logger = LogManager.getLogger(TransformConfigAutoMigration.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(TransformConfigAutoMigration.class);
    private final TransformConfigManager transformConfigManager;
    private final TransformAuditor auditor;
    private final TransformMeterRegistry transformMeterRegistry;
    private final ThreadPool threadPool;

    TransformConfigAutoMigration(
        TransformConfigManager transformConfigManager,
        TransformAuditor auditor,
        TransformMeterRegistry transformMeterRegistry,
        ThreadPool threadPool
    ) {
        this.transformConfigManager = transformConfigManager;
        this.auditor = auditor;
        this.transformMeterRegistry = transformMeterRegistry;
        this.threadPool = threadPool;
    }

    public TransformConfig migrate(TransformConfig currentConfig) {
        // most transforms shouldn't need to migrate, so let's exit early
        if (currentConfig.shouldAutoMigrateMaxPageSearchSize() == false) {
            return currentConfig;
        }

        var updatedConfig = TransformConfig.migrateMaxPageSearchSize(new TransformConfig.Builder(currentConfig)).build();

        var validationException = updatedConfig.validate(null);
        if (validationException == null) {
            auditor.info(updatedConfig.getId(), TransformMessages.MAX_PAGE_SEARCH_SIZE_MIGRATION);
            transformMeterRegistry.autoMigrationCount().increment();
            deprecationLogger.warn(
                DeprecationCategory.API,
                TransformField.MAX_PAGE_SEARCH_SIZE.getPreferredName(),
                TransformMessages.MAX_PAGE_SEARCH_SIZE_MIGRATION
            );
            return updatedConfig;
        } else {
            logger.atDebug()
                .withThrowable(validationException)
                .log(
                    "Failed to validate auto-migrated Config. Please use the _update API to correct and update "
                        + "the Transform configuration. Continuing with old config."
                );
            return currentConfig;
        }
    }

    public void migrateAndSave(TransformConfig currentConfig, ActionListener<TransformConfig> listener) {
        // most transforms shouldn't need to migrate, so let's exit early
        if (currentConfig.shouldAutoMigrateMaxPageSearchSize() == false) {
            listener.onResponse(currentConfig);
            return;
        }

        SubscribableListener.<Tuple<TransformConfig, SeqNoPrimaryTermAndIndex>>newForked(l -> {
            transformConfigManager.getTransformConfigurationForUpdate(currentConfig.getId(), l);
        }).<TransformConfig>andThen(threadPool.generic(), threadPool.getThreadContext(), (l, configAndVersion) -> {
            var updatedConfig = migrate(configAndVersion.v1());
            if (configAndVersion.v1() == updatedConfig) {
                l.onResponse(currentConfig);
            } else {
                transformConfigManager.updateTransformConfiguration(updatedConfig, configAndVersion.v2(), l.map(ignored -> updatedConfig));
            }
        }).addListener(listener.delegateResponse((l, e) -> {
            var errorMessage = "Failed to auto-migrate Config. Please see Elasticsearch logs. Continuing with old config.";
            logger.atWarn().withThrowable(e).log(errorMessage);
            auditor.warning(currentConfig.getId(), errorMessage);
            l.onResponse(currentConfig);
        }), threadPool.generic(), threadPool.getThreadContext());
    }
}
