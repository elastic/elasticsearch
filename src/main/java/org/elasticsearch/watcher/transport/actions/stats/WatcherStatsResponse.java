/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.stats;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.watcher.WatcherBuild;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.WatcherVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class WatcherStatsResponse extends ActionResponse {

    private WatcherVersion version;
    private WatcherBuild build;
    private long watchesCount;
    private WatcherService.State watchServiceState;
    private long watchExecutionQueueSize;
    private long watchExecutionQueueMaxSize;

    WatcherStatsResponse() {
    }

    /**
     * @return The current watch execution queue size
     */
    public long getExecutionQueueSize() {
        return watchExecutionQueueSize;
    }

    void setWatchExecutionQueueSize(long watchExecutionQueueSize) {
        this.watchExecutionQueueSize = watchExecutionQueueSize;
    }

    /**
     * @return The max size of the watch execution queue
     */
    public long getWatchExecutionQueueMaxSize() {
        return watchExecutionQueueMaxSize;
    }

    void setWatchExecutionQueueMaxSize(long watchExecutionQueueMaxSize) {
        this.watchExecutionQueueMaxSize = watchExecutionQueueMaxSize;
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
    public WatcherService.State getWatchServiceState() {
        return watchServiceState;
    }

    void setWatchServiceState(WatcherService.State watcherServiceState) {
        this.watchServiceState = watcherServiceState;
    }

    /**
     * @return The watcher plugin version.
     */
    public WatcherVersion getVersion() {
        return version;
    }

    void setVersion(WatcherVersion version) {
        this.version = version;
    }

    /**
     * @return The watcher plugin build information.
     */
    public WatcherBuild getBuild() {
        return build;
    }

    void setBuild(WatcherBuild build) {
        this.build = build;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        watchesCount = in.readLong();
        watchExecutionQueueSize = in.readLong();
        watchExecutionQueueMaxSize = in.readLong();
        watchServiceState = WatcherService.State.fromId(in.readByte());
        version = WatcherVersion.readVersion(in);
        build = WatcherBuild.readBuild(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(watchesCount);
        out.writeLong(watchExecutionQueueSize);
        out.writeLong(watchExecutionQueueMaxSize);
        out.writeByte(watchServiceState.getId());
        WatcherVersion.writeVersion(version, out);
        WatcherBuild.writeBuild(build, out);
    }
}
