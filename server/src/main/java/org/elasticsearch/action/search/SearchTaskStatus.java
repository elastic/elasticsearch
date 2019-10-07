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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

public class SearchTaskStatus implements Task.Status {

    public static final String NAME = "search_task_status";

    private final boolean readOnly;
    private final List<PhaseInfo> phases;
    private final AtomicReference<PhaseInfo> currentPhase;

    SearchTaskStatus() {
        this.phases = new CopyOnWriteArrayList<>();
        this.currentPhase = new AtomicReference<>();
        this.readOnly = false;
    }

    public SearchTaskStatus(StreamInput in) throws IOException {
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

    void phaseStarted(String phase, int expectedOps) {
        assert readOnly == false;
        boolean result = currentPhase.compareAndSet(null, new PhaseInfo(phase, expectedOps));
        assert result : "another previous phase has not normally completed";
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
        current.shardProcessed(searchPhaseResult.getSearchShardTarget().getShardId(), searchPhaseResult.getTaskInfo());
    }

    void shardFailed(String phase, ShardId shardId, Exception e) {
        assert readOnly == false;
        PhaseInfo current = currentPhase.get();
        assert current != null : "no current phase running, though shards are being reported failed for [" + phase + "]";
        assert phase.equals(current.name) : "phase mismatch: current phase is [" + current.name +
            "] while shards are being reported failed for [" + phase + "]";
        current.shardFailed(shardId, e);
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

    private static class PhaseInfo implements ToXContentFragment, Writeable {
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

        PhaseInfo(String name, Throwable t) {
            this.name = name;
            this.expectedOps = -1;
            this.processed = new CopyOnWriteArrayList<>();
            this.failure = t;
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

        void shardProcessed(ShardId shardId, TaskInfo taskInfo) {
            processed.add(new ShardInfo(shardId, taskInfo));
        }

        void shardFailed(ShardId shardId, Exception e) {
            processed.add(new ShardInfo(shardId, e));
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
            }
            builder.endObject();
            return builder;
        }
    }

    private static class ShardInfo implements Writeable, ToXContentObject {
        private final ShardId shardId;
        @Nullable
        private final Exception exception;
        @Nullable
        private final TaskInfo taskInfo;

        ShardInfo(ShardId shardId, TaskInfo taskInfo) {
            this.shardId = shardId;
            this.taskInfo = taskInfo;
            this.exception = null;
        }

        ShardInfo(ShardId shardId, Exception exception) {
            this.shardId = shardId;
            this.exception = exception;
            this.taskInfo = null;
        }

        ShardInfo(StreamInput in) throws IOException {
            this.shardId = new ShardId(in);
            this.taskInfo = in.readOptionalWriteable(TaskInfo::new);
            this.exception = in.readException();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            this.shardId.writeTo(out);
            out.writeOptionalWriteable(this.taskInfo);
            out.writeException(exception);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            {
                builder.field("index_name", shardId.getIndex().getName());
                builder.field("index_uuid", shardId.getIndex().getUUID());
                builder.field("shard_id", shardId.getId());
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
    }
}
