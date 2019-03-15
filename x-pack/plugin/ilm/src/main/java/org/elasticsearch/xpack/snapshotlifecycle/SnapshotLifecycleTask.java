/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.indexlifecycle.LifecyclePolicySecurityClient;

import java.util.Optional;

public class SnapshotLifecycleTask implements SchedulerEngine.Listener {

    private static Logger logger = LogManager.getLogger(SnapshotLifecycleTask.class);

    private final Client client;
    private final ClusterService clusterService;

    public SnapshotLifecycleTask(final Client client, final ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        logger.debug("snapshot lifecycle policy task triggered from job [{}]", event.getJobName());
        Optional<SnapshotLifecyclePolicyMetadata> maybeMetadata = getSnapPolicyMetadata(event.getJobName(), clusterService.state());
        // If we were on JDK 9 and could use ifPresentOrElse this would be simpler.
        boolean successful = maybeMetadata.map(policyMetadata -> {
            CreateSnapshotRequest request = policyMetadata.getPolicy().toRequest();
            final LifecyclePolicySecurityClient clientWithHeaders = new LifecyclePolicySecurityClient(this.client,
                ClientHelper.INDEX_LIFECYCLE_ORIGIN, policyMetadata.getHeaders());
            logger.info("triggering periodic snapshot for policy [{}]", policyMetadata.getPolicy().getId());
            clientWithHeaders.admin().cluster().createSnapshot(request, new ActionListener<CreateSnapshotResponse>() {
                @Override
                public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                    // TODO: persist this information in cluster state somewhere
                    logger.info("snapshot response for [{}]: {}",
                        policyMetadata.getPolicy().getId(), Strings.toString(createSnapshotResponse));
                }

                @Override
                public void onFailure(Exception e) {
                    // TODO: persist the failure information in cluster state somewhere
                    logger.error("failed to issue create snapshot request for snapshot lifecycle policy " +
                        policyMetadata.getPolicy().getId(), e);
                }
            });
            return true;
        }).orElse(false);

        if (successful == false) {
            logger.warn("snapshot lifecycle policy for job [{}] no longer exists, snapshot not created", event.getJobName());
        }
    }

    /**
     * For the given job id, return an optional policy metadata object, if one exists
     */
    static Optional<SnapshotLifecyclePolicyMetadata> getSnapPolicyMetadata(final String jobId, final ClusterState state) {
       return Optional.ofNullable((SnapshotLifecycleMetadata) state.metaData().custom(SnapshotLifecycleMetadata.TYPE))
           .map(SnapshotLifecycleMetadata::getSnapshotConfigurations)
           .flatMap(configMap -> configMap.values().stream()
               .filter(policyMeta -> jobId.equals(SnapshotLifecycleService.getJobId(policyMeta)))
               .findFirst());
    }
}
