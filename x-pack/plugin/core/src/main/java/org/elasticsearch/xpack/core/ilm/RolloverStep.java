/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;

import java.util.Locale;
import java.util.Objects;

/**
 * Unconditionally rolls over an index using the Rollover API.
 */
public class RolloverStep extends AsyncActionStep {
    private static final Logger logger = LogManager.getLogger(RolloverStep.class);

    public static final String NAME = "attempt-rollover";

    public RolloverStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentClusterState,
                              ClusterStateObserver observer, Listener listener) {
        boolean indexingComplete = LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE_SETTING.get(indexMetaData.getSettings());
        if (indexingComplete) {
            logger.trace(indexMetaData.getIndex() + " has lifecycle complete set, skipping " + RolloverStep.NAME);
            listener.onResponse(true);
            return;
        }

        String rolloverAlias = RolloverAction.LIFECYCLE_ROLLOVER_ALIAS_SETTING.get(indexMetaData.getSettings());

        if (Strings.isNullOrEmpty(rolloverAlias)) {
            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT,
                "setting [%s] for index [%s] is empty or not defined", RolloverAction.LIFECYCLE_ROLLOVER_ALIAS,
                indexMetaData.getIndex().getName())));
            return;
        }

        if (indexMetaData.getAliases().containsKey(rolloverAlias) == false) {
            listener.onFailure(new IllegalArgumentException(String.format(Locale.ROOT,
                "%s [%s] does not point to index [%s]", RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, rolloverAlias,
                indexMetaData.getIndex().getName())));
            return;
        }

        // Calling rollover with no conditions will always roll over the index
        RolloverRequest rolloverRequest = new RolloverRequest(rolloverAlias, null);
        getClient().admin().indices().rolloverIndex(rolloverRequest,
            ActionListener.wrap(response -> {
                assert response.isRolledOver() : "the only way this rollover call should fail is with an exception";
                listener.onResponse(response.isRolledOver());
            }, listener::onFailure));
    }


    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        RolloverStep other = (RolloverStep) obj;
        return super.equals(obj);
    }
}
