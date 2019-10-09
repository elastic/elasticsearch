/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Incorporates the status of a search task running on the coordinate node. Allows to monitor the progress of a search including
 * which phases were completed, which one is running, and for each phase how many shards is it expected to run and which ones have
 * they already been processed.
 */
public class MainSearchTaskStatus implements Task.Status {

    public static final String NAME = "search_task_status";

    private final boolean readOnly;
    private final List<PhaseInfo> phases;
    private final AtomicReference<PhaseInfo> currentPhase;

    MainSearchTaskStatus() {
        this.phases = new CopyOnWriteArrayList<>();
        this.currentPhase = new AtomicReference<>();
        this.readOnly = false;
    }

    public MainSearchTaskStatus(StreamInput in) throws IOException {
        this.currentPhase = new AtomicReference<>(in.readOptionalWriteable(PhaseInfo::new));
        this.phases = in.readList(PhaseInfo::new);
        this.readOnly = true;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(currentPhase.get());
        out.writeList(phases);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public List<PhaseInfo> getCompletedPhases() {
        return phases;
    }

    public PhaseInfo getCurrentPhase() {
        return currentPhase.get();
    }

    void phaseStarted(String phase, int expectedOps) {
        assert readOnly == false;
        boolean result = currentPhase.compareAndSet(null, new PhaseInfo(phase, expectedOps));
        assert result : "phase [" + currentPhase.get().getName() + "] has not normally completed, unable to start [" + phase + "]";
    }

    void phaseFailed(String phase, Throwable t) {
        assert readOnly == false;
        PhaseInfo current = currentPhase.getAndSet(null);
        assert current != null : "no current phase running, though [" + phase + "] has failed";
        assert current.name.equals(phase) : "current phase [" + current.name +
                "] is not the phase that's failed [" + phase + "]";
        PhaseInfo phaseInfo = new PhaseInfo(current, t);
        phases.add(phaseInfo);
    }

    void phaseCompleted(String phase) {
        assert readOnly == false;
        PhaseInfo current = currentPhase.getAndSet(null);
        assert current != null : "no current phase running, though [" + phase + "] has completed";
        assert current.name.equals(phase) : "current phase [" + current.name +
            "] is not the phase that's been completed [" + phase + "]";
        phases.add(current);
    }

    void shardProcessed(String phase, SearchPhaseResult searchPhaseResult) {
        assert readOnly == false;
        PhaseInfo current = currentPhase.get();
        assert current != null : "no current phase running, though shards are being report processed for [" + phase + "]";
        assert phase.equals(current.name) : "phase mismatch: current phase is [" + current.name +
            "] while shards are being reported processed for [" + phase + "]";
        current.shardProcessed(searchPhaseResult.getSearchShardTarget(), searchPhaseResult.getTaskInfo());
    }

    void shardFailed(String phase, SearchShardTarget searchShardTarget, Exception e) {
        assert readOnly == false;
        PhaseInfo current = currentPhase.get();
        assert current != null : "no current phase running, though shards are being reported failed for [" + phase + "]";
        assert phase.equals(current.name) : "phase mismatch: current phase is [" + current.name +
            "] while shards are being reported failed for [" + phase + "]";
        current.shardFailed(searchShardTarget, e);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            PhaseInfo phaseInfo = currentPhase.get();
            if (phaseInfo != null) {
                builder.startObject("phase_running");
                phaseInfo.toXContent(builder, params);
                builder.endObject();
            }
            builder.startArray("phases_completed");
            for (PhaseInfo phase : phases) {
                builder.startObject();
                phase.toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

    public static class PhaseInfo implements ToXContentFragment, Writeable {
        private final String name;
        private final int expectedOps;
        private final List<ShardInfo> processed;
        private final Throwable failure;

        PhaseInfo(String name, int expectedOps) {
            this.name = name;
            this.expectedOps = expectedOps;
            this.processed = new CopyOnWriteArrayList<>();
            this.failure = null;
        }

        PhaseInfo(PhaseInfo partialPhase, Throwable t) {
            this.name = partialPhase.name;
            this.expectedOps = partialPhase.expectedOps;
            this.processed = partialPhase.processed;
            this.failure = t;
        }

        PhaseInfo(StreamInput in) throws IOException {
            this.name = in.readString();
            this.expectedOps = in.readVInt();
            this.processed = in.readList(ShardInfo::new);
            this.failure = in.readException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVInt(expectedOps);
            out.writeList(processed);
            out.writeException(failure);
        }

        public String getName() {
            return name;
        }

        public int getExpectedOps() {
            return expectedOps;
        }

        public List<ShardInfo> getProcessedShards() {
            return processed;
        }

        public Throwable getFailure() {
            return failure;
        }

        void shardProcessed(SearchShardTarget searchShardTarget, TaskInfo taskInfo) {
            processed.add(new ShardInfo(searchShardTarget, taskInfo));
        }

        void shardFailed(SearchShardTarget searchShardTarget, Exception e) {
            processed.add(new ShardInfo(searchShardTarget, e));
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            {
                builder.field("total_shards", expectedOps);
                builder.field("processed_shards", processed.size());
                builder.startArray("processed");
                for (ShardInfo shardInfo : processed) {
                    shardInfo.toXContent(builder, params);
                }
                builder.endArray();
                if (failure != null) {
                    builder.startObject("failure");
                    ElasticsearchException.generateThrowableXContent(builder, params, failure);
                    builder.endObject();
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "PhaseInfo{name='" + name + "'}";
        }
    }

    //TODO shall we keep track of which shard copies have failed? At the moment we know which shard was successful, and in case of failures,
    //we have info for the last shard that failed as part of the replica set.
    public static class ShardInfo implements Writeable, ToXContentObject {
        private final SearchShardTarget searchShardTarget;
        @Nullable
        private final Exception exception;
        @Nullable
        private final TaskInfo taskInfo;

        ShardInfo(SearchShardTarget searchShardTarget, TaskInfo taskInfo) {
            this.searchShardTarget = searchShardTarget;
            this.taskInfo = taskInfo;
            this.exception = null;
        }

        ShardInfo(SearchShardTarget searchShardTarget, Exception exception) {
            this.searchShardTarget = searchShardTarget;
            this.exception = exception;
            this.taskInfo = null;
        }

        ShardInfo(StreamInput in) throws IOException {
            this.searchShardTarget = new SearchShardTarget(in);
            this.taskInfo = in.readOptionalWriteable(TaskInfo::new);
            this.exception = in.readException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.searchShardTarget.writeTo(out);
            out.writeOptionalWriteable(this.taskInfo);
            out.writeException(exception);
        }

        public SearchShardTarget getSearchShardTarget() {
            return searchShardTarget;
        }

        public Exception getFailure() {
            return exception;
        }

        public TaskInfo getTaskInfo() {
            return taskInfo;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("index_name", searchShardTarget.getFullyQualifiedIndexName());
                ShardId shardId = searchShardTarget.getShardId();
                builder.field("index_uuid", shardId.getIndex().getUUID());
                builder.field("shard_id", shardId.getId());
                builder.field("node_id", searchShardTarget.getNodeId());
                if (taskInfo != null) {
                    builder.startObject("task_info");
                    taskInfo.toXContent(builder, params);
                    builder.endObject();
                }
                if (exception != null) {
                    builder.startObject("failure");
                    ElasticsearchException.generateFailureXContent(builder, params, exception, true);
                    builder.endObject();
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "ShardInfo{" +
                "searchShardTarget=" + searchShardTarget +
                '}';
        }
    }
}
