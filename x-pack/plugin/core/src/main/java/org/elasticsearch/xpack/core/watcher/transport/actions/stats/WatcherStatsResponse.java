/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.transport.actions.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.WatcherMetaData;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.core.watcher.execution.QueuedWatch;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionSnapshot;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class WatcherStatsResponse extends BaseNodesResponse<WatcherStatsResponse.Node>
        implements ToXContentObject {

    private WatcherMetaData watcherMetaData;

    public WatcherStatsResponse(StreamInput in) throws IOException {
        super(in);
        watcherMetaData = new WatcherMetaData(in.readBoolean());
    }

    public WatcherStatsResponse(ClusterName clusterName, WatcherMetaData watcherMetaData,
                                List<Node> nodes, List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
        this.watcherMetaData = watcherMetaData;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(watcherMetaData.manuallyStopped());
    }

    @Override
    protected List<Node> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(Node::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<Node> nodes) throws IOException {
        out.writeList(nodes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        watcherMetaData.toXContent(builder, params);
        builder.startArray("stats");
        for (Node node : getNodes()) {
            node.toXContent(builder, params);
        }
        builder.endArray();

        return builder;
    }

    /**
     * Sum all watches across all nodes to get a total count of watches in the cluster
     *
     * @return The sum of all watches being executed
     */
    public long getWatchesCount() {
        return getNodes().stream().mapToLong(WatcherStatsResponse.Node::getWatchesCount).sum();
    }

    public WatcherMetaData watcherMetaData() {
        return watcherMetaData;
    }

    public static class Node extends BaseNodeResponse implements ToXContentObject {

        private long watchesCount;
        private WatcherState watcherState;
        private long threadPoolQueueSize;
        private long threadPoolMaxSize;
        private List<WatchExecutionSnapshot> snapshots;
        private List<QueuedWatch> queuedWatches;
        private Counters stats;

        public Node(StreamInput in) throws IOException {
            super(in);
            watchesCount = in.readLong();
            threadPoolQueueSize = in.readLong();
            threadPoolMaxSize = in.readLong();
            watcherState = WatcherState.fromId(in.readByte());

            if (in.readBoolean()) {
                snapshots = in.readList(WatchExecutionSnapshot::new);
            }
            if (in.readBoolean()) {
                queuedWatches = in.readList(QueuedWatch::new);
            }
            if (in.readBoolean()) {
                stats = new Counters(in);
            }
        }

        public Node(DiscoveryNode node) {
            super(node);
        }

        /**
         * @return The current execution thread pool queue size
         */
        public long getThreadPoolQueueSize() {
            return threadPoolQueueSize;
        }

        public void setThreadPoolQueueSize(long threadPoolQueueSize) {
            this.threadPoolQueueSize = threadPoolQueueSize;
        }

        /**
         * @return The max number of threads in the execution thread pool
         */
        public long getThreadPoolMaxSize() {
            return threadPoolMaxSize;
        }

        public void setThreadPoolMaxSize(long threadPoolMaxSize) {
            this.threadPoolMaxSize = threadPoolMaxSize;
        }

        /**
         * @return The number of watches currently registered in the system
         */
        public long getWatchesCount() {
            return watchesCount;
        }

        public void setWatchesCount(long watchesCount) {
            this.watchesCount = watchesCount;
        }

        /**
         * @return The state of the watch service.
         */
        public WatcherState getWatcherState() {
            return watcherState;
        }

        public void setWatcherState(WatcherState watcherServiceState) {
            this.watcherState = watcherServiceState;
        }

        @Nullable
        public List<WatchExecutionSnapshot> getSnapshots() {
            return snapshots;
        }

        public void setSnapshots(List<WatchExecutionSnapshot> snapshots) {
            this.snapshots = snapshots;
        }

        @Nullable
        public List<QueuedWatch> getQueuedWatches() {
            return queuedWatches;
        }

        public void setQueuedWatches(List<QueuedWatch> queuedWatches) {
            this.queuedWatches = queuedWatches;
        }

        public Counters getStats() {
            return stats;
        }

        public void setStats(Counters stats) {
            this.stats = stats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(watchesCount);
            out.writeLong(threadPoolQueueSize);
            out.writeLong(threadPoolMaxSize);
            out.writeByte(watcherState.getId());

            out.writeBoolean(snapshots != null);
            if (snapshots != null) {
                out.writeList(snapshots);
            }
            out.writeBoolean(queuedWatches != null);
            if (queuedWatches != null) {
                out.writeList(queuedWatches);
            }
            out.writeBoolean(stats != null);
            if (stats != null) {
                stats.writeTo(out);
            }
        }


        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params)
                throws IOException {
            builder.startObject();
            builder.field("node_id", getNode().getId());
            builder.field("watcher_state", watcherState.toString().toLowerCase(Locale.ROOT));
            builder.field("watch_count", watchesCount);
            builder.startObject("execution_thread_pool");
            builder.field("queue_size", threadPoolQueueSize);
            builder.field("max_size", threadPoolMaxSize);
            builder.endObject();

            if (snapshots != null) {
                builder.startArray("current_watches");
                for (WatchExecutionSnapshot snapshot : snapshots) {
                    snapshot.toXContent(builder, params);
                }
                builder.endArray();
            }
            if (queuedWatches != null) {
                builder.startArray("queued_watches");
                for (QueuedWatch queuedWatch : queuedWatches) {
                    queuedWatch.toXContent(builder, params);
                }
                builder.endArray();
            }
            if (stats != null && stats.hasCounters()) {
                builder.field("stats", stats.toNestedMap());
            }
            builder.endObject();
            return builder;
        }
    }
}
