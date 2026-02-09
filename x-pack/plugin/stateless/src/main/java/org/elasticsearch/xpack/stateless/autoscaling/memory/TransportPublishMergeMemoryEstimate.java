/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.autoscaling.memory;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.stateless.autoscaling.memory.MemoryMetricsService.ShardMergeMemoryEstimatePublication;
import org.elasticsearch.xpack.stateless.autoscaling.memory.MergeMemoryEstimateCollector.ShardMergeMemoryEstimate;

import java.io.IOException;
import java.util.Objects;

public class TransportPublishMergeMemoryEstimate extends TransportMasterNodeAction<
    TransportPublishMergeMemoryEstimate.Request,
    ActionResponse.Empty> {

    public static final String NAME = "cluster:monitor/stateless/autoscaling/publish_merge_memory_estimate";
    public static final ActionType<ActionResponse.Empty> INSTANCE = new ActionType<>(NAME);

    private final MemoryMetricsService memoryMetricsService;

    @Inject
    public TransportPublishMergeMemoryEstimate(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final MemoryMetricsService memoryMetricsService
    ) {
        super(
            NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            Request::new,
            in -> ActionResponse.Empty.INSTANCE,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.memoryMetricsService = memoryMetricsService;
    }

    @Override
    protected void masterOperation(Task task, Request request, ClusterState state, ActionListener<ActionResponse.Empty> listener)
        throws Exception {
        ActionListener.completeWith(listener, () -> {
            memoryMetricsService.updateMergeMemoryEstimate(
                new ShardMergeMemoryEstimatePublication(request.seqNo, request.nodeEphemeralId, request.estimate)
            );
            return ActionResponse.Empty.INSTANCE;
        });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return null;
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final long seqNo;
        private final String nodeEphemeralId;
        private final ShardMergeMemoryEstimate estimate;

        public Request(long seqNo, String nodeEphemeralId, ShardMergeMemoryEstimate estimate) {
            super(INFINITE_MASTER_NODE_TIMEOUT);
            this.seqNo = seqNo;
            this.nodeEphemeralId = Objects.requireNonNull(nodeEphemeralId);
            this.estimate = estimate;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            seqNo = in.readVLong();
            nodeEphemeralId = in.readString();
            estimate = new ShardMergeMemoryEstimate(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(seqNo);
            out.writeString(nodeEphemeralId);
            estimate.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public ShardMergeMemoryEstimate getEstimate() {
            return estimate;
        }

        public String getNodeEphemeralId() {
            return nodeEphemeralId;
        }

        public long getSeqNo() {
            return seqNo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o instanceof Request == false) return false;
            Request request = (Request) o;
            return Objects.equals(nodeEphemeralId, request.nodeEphemeralId)
                && Objects.equals(estimate, request.estimate)
                && seqNo == request.seqNo;
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeEphemeralId, estimate, seqNo);
        }

        @Override
        public String toString() {
            return "PublishMergeMemoryEstimateRequest{"
                + "seqNo="
                + seqNo
                + ", estimate="
                + estimate
                + ", nodeEphemeralId='"
                + nodeEphemeralId
                + '\''
                + '}';
        }
    }
}
