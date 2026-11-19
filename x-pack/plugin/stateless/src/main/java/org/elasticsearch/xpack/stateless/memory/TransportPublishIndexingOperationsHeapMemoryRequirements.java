/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

public class TransportPublishIndexingOperationsHeapMemoryRequirements extends TransportMasterNodeAction<
    TransportPublishIndexingOperationsHeapMemoryRequirements.Request,
    ActionResponse.Empty> {

    public static final String NAME = "cluster:monitor/stateless/autoscaling/publish_indexing_operations_heap_memory_requirements";
    public static final ActionType<ActionResponse.Empty> INSTANCE = new ActionType<>(NAME);

    /**
     * Adds the raw indexing operation size so a master can convert it with its own
     * {@code indexing_pressure.memory.max_operation_size}. Older nodes still exchange only the precomputed heap.
     */
    public static final TransportVersion PUBLISH_LARGEST_INDEXING_OPERATION_SIZE = TransportVersion.fromName(
        "publish_largest_indexing_operation_size"
    );

    private final StatelessMemoryMetricsService memoryMetricsService;

    @Inject
    public TransportPublishIndexingOperationsHeapMemoryRequirements(
        final TransportService transportService,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ActionFilters actionFilters,
        final StatelessMemoryMetricsService memoryMetricsService
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
            if (request.hasLargestOperationSize()) {
                // The precomputed heap is only for masters that cannot read the raw size.
                memoryMetricsService.updateLargestIndexingOperationSize(request.getLargestOperationSizeInBytes());
            } else {
                memoryMetricsService.updateIndexingOperationsHeapMemoryRequirements(request.getMinimumRequiredHeapInBytes());
            }
            return ActionResponse.Empty.INSTANCE;
        });
    }

    @Override
    protected ClusterBlockException checkBlock(Request request, ClusterState state) {
        return null;
    }

    public static class Request extends MasterNodeRequest<Request> {
        private final long minimumRequiredHeapInBytes;
        private final long largestOperationSizeInBytes;
        private final boolean hasLargestOperationSize;

        /**
         * A publication from a node that only knows how to send a precomputed heap requirement.
         */
        public Request(long minimumRequiredHeapInBytes) {
            this(minimumRequiredHeapInBytes, 0L, false);
        }

        /**
         * @param minimumRequiredHeapInBytes heap computed on the publishing node, written for masters that do not
         *                                   understand {@code largestOperationSizeInBytes}
         * @param largestOperationSizeInBytes raw size of the largest indexing operation. Masters that understand it
         *                                    ignore {@code minimumRequiredHeapInBytes}.
         */
        public Request(long minimumRequiredHeapInBytes, long largestOperationSizeInBytes) {
            this(minimumRequiredHeapInBytes, largestOperationSizeInBytes, true);
        }

        private Request(long minimumRequiredHeapInBytes, long largestOperationSizeInBytes, boolean hasLargestOperationSize) {
            super(TimeValue.MINUS_ONE);
            this.minimumRequiredHeapInBytes = minimumRequiredHeapInBytes;
            this.largestOperationSizeInBytes = largestOperationSizeInBytes;
            this.hasLargestOperationSize = hasLargestOperationSize;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            this.minimumRequiredHeapInBytes = in.readVLong();
            if (in.getTransportVersion().supports(PUBLISH_LARGEST_INDEXING_OPERATION_SIZE)) {
                this.hasLargestOperationSize = in.readBoolean();
                this.largestOperationSizeInBytes = this.hasLargestOperationSize ? in.readVLong() : 0L;
            } else {
                this.hasLargestOperationSize = false;
                this.largestOperationSizeInBytes = 0L;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(minimumRequiredHeapInBytes);
            if (out.getTransportVersion().supports(PUBLISH_LARGEST_INDEXING_OPERATION_SIZE)) {
                out.writeBoolean(hasLargestOperationSize);
                if (hasLargestOperationSize) {
                    out.writeVLong(largestOperationSizeInBytes);
                }
            }
        }

        public long getMinimumRequiredHeapInBytes() {
            return minimumRequiredHeapInBytes;
        }

        public long getLargestOperationSizeInBytes() {
            return largestOperationSizeInBytes;
        }

        public boolean hasLargestOperationSize() {
            return hasLargestOperationSize;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Request request = (Request) o;
            return minimumRequiredHeapInBytes == request.minimumRequiredHeapInBytes
                && largestOperationSizeInBytes == request.largestOperationSizeInBytes
                && hasLargestOperationSize == request.hasLargestOperationSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(minimumRequiredHeapInBytes, largestOperationSizeInBytes, hasLargestOperationSize);
        }
    }
}
