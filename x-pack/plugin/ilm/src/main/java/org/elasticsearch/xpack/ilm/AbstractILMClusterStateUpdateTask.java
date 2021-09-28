/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

public abstract class AbstractILMClusterStateUpdateTask extends ClusterStateUpdateTask {

    private static final Logger logger = LogManager.getLogger(AbstractILMClusterStateUpdateTask.class);

    private final ListenableFuture<Void> listener = new ListenableFuture<>();

    @Override
    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
        try {
            listener.onResponse(null);
        } catch (Exception e) {
            assert false : e;
            logError(source, e);
        }
    }

    @Override
    public void onFailure(String source, Exception e) {
        try {
            listener.onFailure(e);
        } catch (Exception ex) {
            ex.addSuppressed(e);
            assert false : ex;
            logError(source, e);
        }
    }

    public void addListener(ActionListener<Void> listener) {
        this.listener.addListener(listener);
    }

    private void logError(String source, Exception e) {
        logger.error(new ParameterizedMessage("unexpected failure in resolving listener for [{}]", source), e);
    }
}
