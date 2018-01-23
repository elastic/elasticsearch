/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.stats;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.WatcherMetaData;
import org.elasticsearch.xpack.core.watcher.WatcherState;
import org.elasticsearch.xpack.core.watcher.execution.QueuedWatch;
import org.elasticsearch.xpack.core.watcher.execution.WatchExecutionSnapshot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class OldWatcherStatsResponse extends ActionResponse implements ToXContentObject {

    private long watchesCount;
    private WatcherState watcherState;
    private long threadPoolQueueSize;
    private long threadPoolMaxSize;
    private WatcherMetaData watcherMetaData;

    private List<WatchExecutionSnapshot> snapshots;
    private List<QueuedWatch> queuedWatches;

    OldWatcherStatsResponse() {
    }

    /**
     * @return The current execution thread pool queue size
     */
    public long getThreadPoolQueueSize() {
        return threadPoolQueueSize;
    }

    void setThreadPoolQueueSize(long threadPoolQueueSize) {
        this.threadPoolQueueSize = threadPoolQueueSize;
    }

    /**
     * @return The max number of threads in the execution thread pool
     */
    public long getThreadPoolMaxSize() {
        return threadPoolMaxSize;
    }

    void setThreadPoolMaxSize(long threadPoolMaxSize) {
        this.threadPoolMaxSize = threadPoolMaxSize;
    }

    /**
     * @return The number of watches currently registered in the system
     */
    public long getWatchesCount() {
        return watchesCount;
    }

    void setWatchesCount(long watchesCount) {
        this.watchesCount = watchesCount;
    }

    /**
     * @return The state of the watch service.
     */
    public WatcherState getWatcherState() {
        return watcherState;
    }

    void setWatcherState(WatcherState watcherServiceState) {
        this.watcherState = watcherServiceState;
    }

    @Nullable
    public List<WatchExecutionSnapshot> getSnapshots() {
        return snapshots;
    }

    void setSnapshots(List<WatchExecutionSnapshot> snapshots) {
        this.snapshots = snapshots;
    }

    @Nullable
    public List<QueuedWatch> getQueuedWatches() {
        return queuedWatches;
    }

    public void setQueuedWatches(List<QueuedWatch> queuedWatches) {
        this.queuedWatches = queuedWatches;
    }

    public WatcherMetaData getWatcherMetaData() {
        return watcherMetaData;
    }

    public void setWatcherMetaData(WatcherMetaData watcherMetaData) {
        this.watcherMetaData = watcherMetaData;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        watchesCount = in.readLong();
        threadPoolQueueSize = in.readLong();
        threadPoolMaxSize = in.readLong();
        watcherState = WatcherState.fromId(in.readByte());

        if (in.readBoolean()) {
            int size = in.readVInt();
            snapshots = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                WatchExecutionSnapshot snapshot = new WatchExecutionSnapshot();
                snapshot.readFrom(in);
                snapshots.add(snapshot);
            }
        }
        if (in.readBoolean()) {
            int size = in.readVInt();
            queuedWatches = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                QueuedWatch queuedWatch = new QueuedWatch();
                queuedWatch.readFrom(in);
                queuedWatches.add(queuedWatch);
            }
        }
        watcherMetaData = new WatcherMetaData(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(watchesCount);
        out.writeLong(threadPoolQueueSize);
        out.writeLong(threadPoolMaxSize);
        out.writeByte(watcherState.getId());

        if (snapshots != null) {
            out.writeBoolean(true);
            out.writeVInt(snapshots.size());
            for (WatchExecutionSnapshot snapshot : snapshots) {
                snapshot.writeTo(out);
            }
        } else {
            out.writeBoolean(false);
        }
        if (queuedWatches != null) {
            out.writeBoolean(true);
            out.writeVInt(queuedWatches.size());
            for (QueuedWatch pending : this.queuedWatches) {
                pending.writeTo(out);
            }
        } else {
            out.writeBoolean(false);
        }
        watcherMetaData.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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
        watcherMetaData.toXContent(builder, params);

        builder.endObject();
        return builder;
    }
}
