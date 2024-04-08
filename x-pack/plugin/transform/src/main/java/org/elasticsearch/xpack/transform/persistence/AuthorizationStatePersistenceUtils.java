/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;

/**
 * {@link AuthorizationStatePersistenceUtils} is a utility class built on top of {@link TransformConfigManager}.
 * It allows fetching and persisting {@link AuthorizationState} objects.
 */
public final class AuthorizationStatePersistenceUtils {

    private static final Logger logger = LogManager.getLogger(AuthorizationStatePersistenceUtils.class);

    /**
     * Fetches persisted authorization state object and returns it back to the caller.
     */
    public static void fetchAuthState(
        TransformConfigManager transformConfigManager,
        String transformId,
        ActionListener<AuthorizationState> listener
    ) {
        ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> getTransformStoredDocListener = ActionListener.wrap(
            stateAndStatsAndSeqNoPrimaryTermAndIndex -> {
                if (stateAndStatsAndSeqNoPrimaryTermAndIndex == null) {
                    listener.onResponse(null);
                    return;
                }
                TransformState transformState = stateAndStatsAndSeqNoPrimaryTermAndIndex.v1().getTransformState();
                if (transformState == null) {
                    listener.onResponse(null);
                    return;
                }
                AuthorizationState authState = transformState.getAuthState();
                if (authState == null) {
                    listener.onResponse(null);
                    return;
                }
                listener.onResponse(authState);
            },
            listener::onFailure
        );

        transformConfigManager.getTransformStoredDoc(transformId, true, getTransformStoredDocListener);
    }

    /**
     * Persists the authorization state object given as an argument.
     */
    public static void persistAuthState(
        Settings settings,
        TransformConfigManager transformConfigManager,
        String transformId,
        AuthorizationState authState,
        ActionListener<Void> listener
    ) {
        assert XPackSettings.SECURITY_ENABLED.get(settings);

        logger.trace("Started persisting auth state: {}", authState);

        ActionListener<SeqNoPrimaryTermAndIndex> persistListener = ActionListener.wrap(unusedResponse -> {
            logger.trace("Finished persisting auth state: {}", authState);
            listener.onResponse(null);
        }, e -> {
            logger.trace("Failed persisting auth state: {}, exception = {}", authState, e);
            listener.onFailure(e);
        });

        ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> transformStatsActionListener = ActionListener.wrap(
            stateAndStatsAndSeqNoPrimaryTermAndIndex -> {
                // Stored doc does not exist yet, we need to create one
                if (stateAndStatsAndSeqNoPrimaryTermAndIndex == null) {
                    TransformState state = new TransformState(
                        TransformTaskState.STOPPED,
                        IndexerState.STOPPED,
                        null,
                        0,
                        null,
                        null,
                        null,
                        false,
                        authState
                    );
                    TransformIndexerStats stats = new TransformIndexerStats();
                    transformConfigManager.putOrUpdateTransformStoredDoc(
                        new TransformStoredDoc(transformId, state, stats),
                        null,
                        persistListener
                    );
                    return;
                }
                // Stored doc already exists, we just need to update its authState field
                TransformStoredDoc stateAndStats = stateAndStatsAndSeqNoPrimaryTermAndIndex.v1();
                SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex = stateAndStatsAndSeqNoPrimaryTermAndIndex.v2();
                TransformState oldState = stateAndStats.getTransformState();
                TransformState newState = new TransformState(
                    oldState.getTaskState(),
                    oldState.getIndexerState(),
                    oldState.getPosition(),
                    oldState.getCheckpoint(),
                    oldState.getReason(),
                    oldState.getProgress(),
                    oldState.getNode(),
                    oldState.shouldStopAtNextCheckpoint(),
                    authState
                );
                TransformIndexerStats stats = stateAndStats.getTransformStats();
                transformConfigManager.putOrUpdateTransformStoredDoc(
                    new TransformStoredDoc(transformId, newState, stats),
                    seqNoPrimaryTermAndIndex,
                    persistListener
                );
            },
            error -> {
                String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_LOAD_TRANSFORM_STATE, transformId);
                logger.error(msg, error);
                listener.onFailure(error);
            }
        );

        transformConfigManager.getTransformStoredDoc(transformId, true, transformStatsActionListener);
    }
}
