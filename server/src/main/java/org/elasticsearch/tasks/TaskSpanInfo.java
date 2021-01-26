/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class TaskSpanInfo implements ToXContentObject, Writeable {
    private final long startTimeInMillis;
    private final long runningTimeInMillis;
    private final boolean completed;
    private final Exception failure;
    private final Map<String, Object> attributes;
    private final List<TaskSpanInfo> subSpans;

    TaskSpanInfo(long startTimeInMillis, long runningTimeInMillis, boolean completed,
                 Exception failure, Map<String, Object> attributes, List<TaskSpanInfo> subSpans) {
        this.startTimeInMillis = startTimeInMillis;
        this.runningTimeInMillis = runningTimeInMillis;
        this.completed = completed;
        this.failure = failure;
        this.attributes = Objects.requireNonNull(attributes);
        this.subSpans = Objects.requireNonNull(subSpans);
    }

    TaskSpanInfo(StreamInput in) throws IOException {
        this.startTimeInMillis = in.readLong();
        this.runningTimeInMillis = in.readLong();
        this.completed = in.readBoolean();
        this.failure = in.readException();
        this.attributes = in.readMap(StreamInput::readString, StreamInput::readGenericValue);
        this.subSpans = in.readList(TaskSpanInfo::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(startTimeInMillis);
        out.writeLong(runningTimeInMillis);
        out.writeBoolean(completed);
        out.writeException(failure);
        out.writeMap(attributes, StreamOutput::writeString, StreamOutput::writeGenericValue);
        out.writeList(subSpans);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (completed) {
            if (failure != null) {
                builder.field("status", "failed");
                builder.startObject("failure");
                ElasticsearchException.generateThrowableXContent(builder, params, failure);
                builder.endObject();
            } else {
                builder.field("status", "completed");
            }
        } else {
            if (attributes.containsKey(TaskSpan.REMOTE_ACTION)) {
                builder.field("status", "waiting");
            } else {
                builder.field("status", "running");
            }
        }
        builder.field("start_time_in_millis", startTimeInMillis);
        builder.field("running_time_in_millis", runningTimeInMillis);
        for (Map.Entry<String, Object> e : attributes.entrySet()) {
            builder.field(e.getKey(), e.getValue());
        }
        if (subSpans.isEmpty() == false) {
            builder.field("children", subSpans);
        }
        builder.endObject();
        return builder;
    }
}
