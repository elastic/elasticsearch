/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.persistent.CompletionPersistentTaskAction;
import org.elasticsearch.persistent.PersistentTaskResponse;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ccr.action.ShardFollowTask;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A {@link ClusterStateListener} that completes any {@link ShardFollowTask} which concerns a deleted index.
 */
public class ShardFollowTaskCleaner implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ShardFollowTaskCleaner.class);

    private final ThreadPool threadPool;
    private final Client client;

    /**
     * Tasks that are currently being completed.
     */
    private final Set<ShardFollowTask> completing = Collections.synchronizedSet(new HashSet<>());

    public ShardFollowTaskCleaner(final ClusterService clusterService, final ThreadPool threadPool, final Client client) {
        this.threadPool = threadPool;
        this.client = client;
        if (DiscoveryNode.isMasterNode(clusterService.getSettings())) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }
        if (event.localNodeMaster() == false) {
            return;
        }
        final Metadata metadata = event.state().metadata();
        final PersistentTasksCustomMetadata persistentTasksMetadata = metadata.custom(PersistentTasksCustomMetadata.TYPE);
        final Metadata previousMetadata = event.previousState().metadata();
        if (metadata.indices() == event.previousState().getMetadata().indices()
            && persistentTasksMetadata == previousMetadata.custom(PersistentTasksCustomMetadata.TYPE)
            && event.previousState().nodes().isLocalNodeElectedMaster()
            && event.blocksChanged() == false) {
            // nothing of relevance changed
            return;
        }

        if (persistentTasksMetadata == null) {
            return;
        }
        for (PersistentTasksCustomMetadata.PersistentTask<?> persistentTask : persistentTasksMetadata.tasks()) {
            if (ShardFollowTask.NAME.equals(persistentTask.getTaskName()) == false) {
                // this task is not a shard follow task
                continue;
            }
            ShardFollowTask shardFollowTask = (ShardFollowTask) persistentTask.getParams();
            Index followerIndex = shardFollowTask.getFollowShardId().getIndex();
            if (metadata.index(followerIndex) != null) {
                // the index exists, do not clean this persistent task
                continue;
            }
            if (completing.add(shardFollowTask) == false) {
                // already completing this task
                continue;
            }
            threadPool.generic().execute(ActionRunnable.wrap(ActionListener.runBefore(new ActionListener<PersistentTaskResponse>() {

                @Override
                public void onResponse(PersistentTaskResponse persistentTaskResponse) {
                    logger.debug("task [{}] cleaned up", persistentTask.getId());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn(() -> "failed to clean up task [" + persistentTask.getId() + "]", e);
                }
            }, () -> completing.remove(shardFollowTask)), listener -> {
                /*
                 * We are executing under the system context, on behalf of the user to clean up the shard follow task after the follower
                 * index was deleted. This is why the system role includes the privilege for persistent task completion.
                 */
                assert threadPool.getThreadContext().isSystemContext();
                client.execute(
                    CompletionPersistentTaskAction.INSTANCE,
                    new CompletionPersistentTaskAction.Request(
                        persistentTask.getId(),
                        persistentTask.getAllocationId(),
                        new IndexNotFoundException(followerIndex),
                        null
                    ),
                    listener
                );
            }));
        }
    }
}
