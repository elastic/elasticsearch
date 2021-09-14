/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.watcher;

import org.elasticsearch.client.NodesResponseHeader;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * The response from an 'ack watch' request.
 */
public class WatcherStatsResponse {

    private final List<Node> nodes;
    private final NodesResponseHeader header;
    private final String clusterName;

    private final WatcherMetadata watcherMetadata;

    public WatcherStatsResponse(NodesResponseHeader header, String clusterName, WatcherMetadata watcherMetadata, List<Node> nodes) {
        this.nodes = nodes;
        this.header = header;
        this.clusterName = clusterName;
        this.watcherMetadata = watcherMetadata;
    }

    /**
     * @return the status of the requested watch. If an action was
     * successfully acknowledged, this will be reflected in its status.
     */
    public WatcherMetadata getWatcherMetadata() {
        return watcherMetadata;
    }

    /**
     * returns a list of nodes that returned stats
     */
    public List<Node> getNodes() {
        return nodes;
    }

    /**
     * Gets information about the number of total, successful and failed nodes the request was run on.
     * Also includes exceptions if relevant.
     */
    public NodesResponseHeader getHeader() {
        return header;
    }

    /**
     * Get the cluster name associated with all of the nodes.
     *
     * @return Never {@code null}.
     */
    public String getClusterName() {
        return clusterName;
    }

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<WatcherStatsResponse, Void> PARSER =
        new ConstructingObjectParser<>("watcher_stats_response", true,
            a -> new WatcherStatsResponse((NodesResponseHeader) a[0], (String) a[1], new WatcherMetadata((boolean) a[2]),
                (List<Node>) a[3]));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), NodesResponseHeader::fromXContent, new ParseField("_nodes"));
        PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("cluster_name"));
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), new ParseField("manually_stopped"));
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), (p, c) -> Node.PARSER.apply(p, null),
            new ParseField("stats"));
    }

    public static WatcherStatsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WatcherStatsResponse that = (WatcherStatsResponse) o;
        return Objects.equals(nodes, that.nodes) &&
            Objects.equals(header, that.header) &&
            Objects.equals(clusterName, that.clusterName) &&
            Objects.equals(watcherMetadata, that.watcherMetadata);
    }

    @Override
    public int hashCode() {

        return Objects.hash(nodes, header, clusterName, watcherMetadata);
    }

    public static class Node {
        @SuppressWarnings("unchecked")
        public static final ConstructingObjectParser<Node, Void> PARSER =
            new ConstructingObjectParser<>("watcher_stats_node", true, (args, c) -> new Node(
                (String) args[0],
                WatcherState.valueOf(((String) args[1]).toUpperCase(Locale.ROOT)),
                (long) args[2],
                ((Tuple<Long, Long>) args[3]).v1(),
                ((Tuple<Long, Long>) args[3]).v2(),
                (List<WatchExecutionSnapshot>) args[4],
                (List<QueuedWatch>) args[5],
                (Map<String, Object>) args[6]

            ));

        private static final ConstructingObjectParser<Tuple<Long, Long>, Void> THREAD_POOL_PARSER =
            new ConstructingObjectParser<>("execution_thread_pool", true, (args, id) -> new Tuple<>((Long) args[0], (Long) args[1]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("node_id"));
            PARSER.declareString(ConstructingObjectParser.constructorArg(), new ParseField("watcher_state"));
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), new ParseField("watch_count"));
            PARSER.declareObject(ConstructingObjectParser.constructorArg(), THREAD_POOL_PARSER::apply,
                new ParseField("execution_thread_pool"));
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), WatchExecutionSnapshot.PARSER,
                new ParseField("current_watches"));
            PARSER.declareObjectArray(ConstructingObjectParser.optionalConstructorArg(), QueuedWatch.PARSER,
                new ParseField("queued_watches"));
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> p.map(), new ParseField("stats"));

            THREAD_POOL_PARSER.declareLong(ConstructingObjectParser.constructorArg(), new ParseField("queue_size"));
            THREAD_POOL_PARSER.declareLong(ConstructingObjectParser.constructorArg(), new ParseField("max_size"));
        }

        private final String nodeId;

        private WatcherState watcherState;
        private long watchesCount;
        private long threadPoolQueueSize;
        private long threadPoolMaxSize;
        private List<WatchExecutionSnapshot> snapshots;
        private List<QueuedWatch> queuedWatches;
        private Map<String, Object> stats;


        public Node(String nodeId, WatcherState watcherState, long watchesCount, long threadPoolQueueSize, long threadPoolMaxSize,
                    List<WatchExecutionSnapshot> snapshots, List<QueuedWatch> queuedWatches, Map<String, Object> stats) {
            this.nodeId = nodeId;
            this.watcherState = watcherState;
            this.watchesCount = watchesCount;
            this.threadPoolQueueSize = threadPoolQueueSize;
            this.threadPoolMaxSize = threadPoolMaxSize;
            this.snapshots = snapshots;
            this.queuedWatches = queuedWatches;
            this.stats = stats;
        }

        public String getNodeId() {
            return nodeId;
        }

        public long getWatchesCount() {
            return watchesCount;
        }

        public WatcherState getWatcherState() {
            return watcherState;
        }

        public long getThreadPoolQueueSize() {
            return threadPoolQueueSize;
        }

        public long getThreadPoolMaxSize() {
            return threadPoolMaxSize;
        }

        public List<WatchExecutionSnapshot> getSnapshots() {
            return snapshots;
        }

        public List<QueuedWatch> getQueuedWatches() {
            return queuedWatches;
        }

        public Map<String, Object> getStats() {
            return stats;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return watchesCount == node.watchesCount &&
                threadPoolQueueSize == node.threadPoolQueueSize &&
                threadPoolMaxSize == node.threadPoolMaxSize &&
                Objects.equals(nodeId, node.nodeId) &&
                watcherState == node.watcherState &&
                Objects.equals(snapshots, node.snapshots) &&
                Objects.equals(queuedWatches, node.queuedWatches) &&
                Objects.equals(stats, node.stats);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, watcherState, watchesCount, threadPoolQueueSize, threadPoolMaxSize, snapshots, queuedWatches,
                stats);
        }
    }
}
