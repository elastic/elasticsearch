/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.indexlifecycle.LifecyclePolicySecurityClient;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;

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
                    logger.info("snapshot response for [{}]: {}",
                        policyMetadata.getPolicy().getId(), Strings.toString(createSnapshotResponse));
                    clusterService.submitStateUpdateTask("slm-record-success-" + policyMetadata.getPolicy().getId(),
                        WriteJobStatus.success(policyMetadata.getPolicy().getId(), request.snapshot(), Instant.now().toEpochMilli()));
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("failed to issue create snapshot request for snapshot lifecycle policy [{}]: {}",
                        policyMetadata.getPolicy().getId(), e);
                    clusterService.submitStateUpdateTask("slm-record-failure-" + policyMetadata.getPolicy().getId(),
                        WriteJobStatus.failure(policyMetadata.getPolicy().getId(), request.snapshot(), Instant.now().toEpochMilli(), e));
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

    private static class WriteJobStatus extends ClusterStateUpdateTask {
        private static final ToXContent.Params STACKTRACE_PARAMS =
            new ToXContent.MapParams(Collections.singletonMap(REST_EXCEPTION_SKIP_STACK_TRACE, "false"));

        private final String policyName;
        private final String snapshotName;
        private final long timestamp;
        private final Optional<Exception> exception;

        private WriteJobStatus(String policyName, String snapshotName, long timestamp, Optional<Exception> exception) {
            this.policyName = policyName;
            this.snapshotName = snapshotName;
            this.exception = exception;
            this.timestamp = timestamp;
        }

        static WriteJobStatus success(String policyId, String snapshotName, long timestamp) {
            return new WriteJobStatus(policyId, snapshotName, timestamp, Optional.empty());
        }

        static WriteJobStatus failure(String policyId, String snapshotName, long timestamp, Exception exception) {
            return new WriteJobStatus(policyId, snapshotName, timestamp, Optional.of(exception));
        }

        private String exceptionToString() throws IOException {
            if (exception.isPresent()) {
                try (XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder()) {
                    causeXContentBuilder.startObject();
                    ElasticsearchException.generateThrowableXContent(causeXContentBuilder, STACKTRACE_PARAMS, exception.get());
                    causeXContentBuilder.endObject();
                    return BytesReference.bytes(causeXContentBuilder).utf8ToString();
                }
            }
            return null;
        }

        @Override
        public ClusterState execute(ClusterState currentState) throws Exception {
            SnapshotLifecycleMetadata snapMeta = currentState.metaData().custom(SnapshotLifecycleMetadata.TYPE);

            assert snapMeta != null : "this should never be called while the snapshot lifecycle cluster metadata is null";
            if (snapMeta == null) {
                logger.error("failed to record snapshot [{}] for snapshot policy [{}]: snapshot lifecycle metadata is null",
                    exception.isPresent() ? "failure" : "success", policyName);
                return currentState;
            }

            Map<String, SnapshotLifecyclePolicyMetadata> snapLifecycles = new HashMap<>(snapMeta.getSnapshotConfigurations());
            SnapshotLifecyclePolicyMetadata policyMetadata = snapLifecycles.get(policyName);
            if (policyMetadata == null) {
                logger.warn("failed to record snapshot [{}] for snapshot policy [{}]: policy not found",
                    exception.isPresent() ? "failure" : "success", policyName);
                return currentState;
            }

            SnapshotLifecyclePolicyMetadata.Builder newPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder(policyMetadata);

            if (exception.isPresent()) {
                newPolicyMetadata.setLastFailure(new SnapshotInvocationRecord(snapshotName, timestamp, exceptionToString()));
            } else {
                newPolicyMetadata.setLastSuccess(new SnapshotInvocationRecord(snapshotName, timestamp, null));
            }

            snapLifecycles.put(policyName, newPolicyMetadata.build());
            SnapshotLifecycleMetadata lifecycleMetadata = new SnapshotLifecycleMetadata(snapLifecycles);
            MetaData currentMeta = currentState.metaData();
            return ClusterState.builder(currentState)
                .metaData(MetaData.builder(currentMeta)
                    .putCustom(SnapshotLifecycleMetadata.TYPE, lifecycleMetadata))
                .build();
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.error("failed to record snapshot policy execution status [{}]: {}", source, e);
        }
    }
}
