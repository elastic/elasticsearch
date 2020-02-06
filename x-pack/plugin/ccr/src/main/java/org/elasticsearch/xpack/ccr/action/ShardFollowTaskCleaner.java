/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.persistent.PersistentTaskResponse;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * A {@link ClusterStateListener} that completes any {@link ShardFollowTask} which concerns a deleted index.
 */
public class ShardFollowTaskCleaner implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ShardFollowTaskCleaner.class);

    private final ThreadPool threadPool;
    private final Client client;

    public ShardFollowTaskCleaner(final ClusterService clusterService, final ThreadPool threadPool, final Client client) {
        this.threadPool = threadPool;
        this.client = client;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (event.localNodeMaster() == false) {
            return;
        }

        MetaData metaData = event.state().metaData();
        PersistentTasksCustomMetaData persistentTasksMetaData = metaData.custom(PersistentTasksCustomMetaData.TYPE);
        if (persistentTasksMetaData == null) {
            return;
        }
        for (PersistentTasksCustomMetaData.PersistentTask<?> persistentTask : persistentTasksMetaData.tasks()) {
            if (ShardFollowTask.NAME.equals(persistentTask.getTaskName()) == false) {
                // this task is not a shard follow task
                continue;
            }
            ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
            Index followerIndex = shardFollowTask.getFollowShardId().getIndex();
            if (metaData.index(followerIndex) != null) {
                // the index exists, do not clean this persistent task
                continue;
            }
            IndexNotFoundException infe = new IndexNotFoundException(followerIndex);
            CompletionPersistentTaskAction.Request request =
                new CompletionPersistentTaskAction.Request(persistentTask.getId(), persistentTask.getAllocationId(), infe);
            threadPool.generic().submit(() -> {
                client.execute(CompletionPersistentTaskAction.INSTANCE, request, new ActionListener<>() {

                    @Override
                    public void onResponse(PersistentTaskResponse persistentTaskResponse) {
                        logger.debug("task [{}] cleaned up", persistentTask.getId());
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(new ParameterizedMessage("failed to clean up task [{}]", persistentTask.getId()), e);
                    }
                });
            });
        }
    }
}
