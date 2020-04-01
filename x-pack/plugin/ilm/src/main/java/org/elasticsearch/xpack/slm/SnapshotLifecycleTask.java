/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.slm.SnapshotInvocationRecord;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicyMetadata;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryItem;
import org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore;
import org.elasticsearch.xpack.ilm.LifecyclePolicySecurityClient;

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
    private final SnapshotHistoryStore historyStore;

    public SnapshotLifecycleTask(final Client client, final ClusterService clusterService, final SnapshotHistoryStore historyStore) {
        this.client = client;
        this.clusterService = clusterService;
        this.historyStore = historyStore;
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        logger.debug("snapshot lifecycle policy task triggered from job [{}]", event.getJobName());

        final Optional<String> snapshotName = maybeTakeSnapshot(event.getJobName(), client, clusterService, historyStore);

        // Would be cleaner if we could use Optional#ifPresentOrElse
        snapshotName.ifPresent(name ->
            logger.info("snapshot lifecycle policy job [{}] issued new snapshot creation for [{}] successfully",
                event.getJobName(), name));

        if (snapshotName.isPresent() == false) {
            logger.warn("snapshot lifecycle policy for job [{}] no longer exists, snapshot not created", event.getJobName());
        }
    }

    /**
     * For the given job id (a combination of policy id and version), issue a create snapshot
     * request. On a successful or failed create snapshot issuing the state is stored in the cluster
     * state in the policy's metadata
     * @return An optional snapshot name if the request was issued successfully
     */
    public static Optional<String> maybeTakeSnapshot(final String jobId, final Client client, final ClusterService clusterService,
                                                     final SnapshotHistoryStore historyStore) {
        Optional<SnapshotLifecyclePolicyMetadata> maybeMetadata = getSnapPolicyMetadata(jobId, clusterService.state());
        String snapshotName = maybeMetadata.map(policyMetadata -> {
            CreateSnapshotRequest request = policyMetadata.getPolicy().toRequest();
            final LifecyclePolicySecurityClient clientWithHeaders = new LifecyclePolicySecurityClient(client,
                ClientHelper.INDEX_LIFECYCLE_ORIGIN, policyMetadata.getHeaders());
            logger.info("snapshot lifecycle policy [{}] issuing create snapshot [{}]",
                policyMetadata.getPolicy().getId(), request.snapshot());
            clientWithHeaders.admin().cluster().createSnapshot(request, new ActionListener<>() {
                @Override
                public void onResponse(CreateSnapshotResponse createSnapshotResponse) {
                    logger.debug("snapshot response for [{}]: {}",
                        policyMetadata.getPolicy().getId(), Strings.toString(createSnapshotResponse));
                    final SnapshotInfo snapInfo = createSnapshotResponse.getSnapshotInfo();

                    // Check that there are no failed shards, since the request may not entirely
                    // fail, but may still have failures (such as in the case of an aborted snapshot)
                    if (snapInfo.failedShards() == 0) {
                        final long timestamp = Instant.now().toEpochMilli();
                        clusterService.submitStateUpdateTask("slm-record-success-" + policyMetadata.getPolicy().getId(),
                            WriteJobStatus.success(policyMetadata.getPolicy().getId(), request.snapshot(), timestamp));
                        historyStore.putAsync(SnapshotHistoryItem.creationSuccessRecord(timestamp, policyMetadata.getPolicy(),
                            request.snapshot()));
                    } else {
                        int failures = snapInfo.failedShards();
                        int total = snapInfo.totalShards();
                        final SnapshotException e = new SnapshotException(request.repository(), request.snapshot(),
                            "failed to create snapshot successfully, " + failures + " out of " + total + " total shards failed");
                        // Add each failed shard's exception as suppressed, the exception contains
                        // information about which shard failed
                        snapInfo.shardFailures().forEach(failure -> e.addSuppressed(failure.getCause()));
                        // Call the failure handler to register this as a failure and persist it
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("failed to create snapshot for snapshot lifecycle policy [{}]: {}",
                        policyMetadata.getPolicy().getId(), e);
                    final long timestamp = Instant.now().toEpochMilli();
                    clusterService.submitStateUpdateTask("slm-record-failure-" + policyMetadata.getPolicy().getId(),
                        WriteJobStatus.failure(policyMetadata.getPolicy().getId(), request.snapshot(), timestamp, e));
                    final SnapshotHistoryItem failureRecord;
                    try {
                        failureRecord = SnapshotHistoryItem.creationFailureRecord(timestamp, policyMetadata.getPolicy(),
                            request.snapshot(), e);
                        historyStore.putAsync(failureRecord);
                    } catch (IOException ex) {
                        // This shouldn't happen unless there's an issue with serializing the original exception, which shouldn't happen
                        logger.error(new ParameterizedMessage(
                            "failed to record snapshot creation failure for snapshot lifecycle policy [{}]",
                            policyMetadata.getPolicy().getId()), e);
                    }
                }
            });
            return request.snapshot();
        }).orElse(null);

        return Optional.ofNullable(snapshotName);
    }

    /**
     * For the given job id, return an optional policy metadata object, if one exists
     */
    static Optional<SnapshotLifecyclePolicyMetadata> getSnapPolicyMetadata(final String jobId, final ClusterState state) {
       return Optional.ofNullable((SnapshotLifecycleMetadata) state.metadata().custom(SnapshotLifecycleMetadata.TYPE))
           .map(SnapshotLifecycleMetadata::getSnapshotConfigurations)
           .flatMap(configMap -> configMap.values().stream()
               .filter(policyMeta -> jobId.equals(SnapshotLifecycleService.getJobId(policyMeta)))
               .findFirst());
    }

    /**
     * A cluster state update task to write the result of a snapshot job to the cluster metadata for the associated policy.
     */
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
            SnapshotLifecycleMetadata snapMeta = currentState.metadata().custom(SnapshotLifecycleMetadata.TYPE);

            assert snapMeta != null : "this should never be called while the snapshot lifecycle cluster metadata is null";
            if (snapMeta == null) {
                logger.error("failed to record snapshot [{}] for snapshot [{}] in policy [{}]: snapshot lifecycle metadata is null",
                    exception.isPresent() ? "failure" : "success", snapshotName, policyName);
                return currentState;
            }

            Map<String, SnapshotLifecyclePolicyMetadata> snapLifecycles = new HashMap<>(snapMeta.getSnapshotConfigurations());
            SnapshotLifecyclePolicyMetadata policyMetadata = snapLifecycles.get(policyName);
            if (policyMetadata == null) {
                logger.warn("failed to record snapshot [{}] for snapshot [{}] in policy [{}]: policy not found",
                    exception.isPresent() ? "failure" : "success", snapshotName, policyName);
                return currentState;
            }

            SnapshotLifecyclePolicyMetadata.Builder newPolicyMetadata = SnapshotLifecyclePolicyMetadata.builder(policyMetadata);
            final SnapshotLifecycleStats stats = snapMeta.getStats();

            if (exception.isPresent()) {
                stats.snapshotFailed(policyName);
                newPolicyMetadata.setLastFailure(new SnapshotInvocationRecord(snapshotName, timestamp, exceptionToString()));
            } else {
                stats.snapshotTaken(policyName);
                newPolicyMetadata.setLastSuccess(new SnapshotInvocationRecord(snapshotName, timestamp, null));
            }

            snapLifecycles.put(policyName, newPolicyMetadata.build());
            SnapshotLifecycleMetadata lifecycleMetadata = new SnapshotLifecycleMetadata(snapLifecycles,
                snapMeta.getOperationMode(), stats);
            Metadata currentMeta = currentState.metadata();
            return ClusterState.builder(currentState)
                .metadata(Metadata.builder(currentMeta)
                    .putCustom(SnapshotLifecycleMetadata.TYPE, lifecycleMetadata))
                .build();
        }

        @Override
        public void onFailure(String source, Exception e) {
            logger.error("failed to record snapshot policy execution status for snapshot [{}] in policy [{}], (source: [{}]): {}",
                snapshotName, policyName, source, e);
        }
    }
}
