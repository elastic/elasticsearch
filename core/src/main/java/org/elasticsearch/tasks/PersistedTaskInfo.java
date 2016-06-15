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
package org.elasticsearch.tasks;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;

/**
 * Information about a persisted or running task. Running tasks just have a {@link #getTask()} while persisted tasks will have either a
 * {@link #getError()} or {@link #getResult()}.
 */
public final class PersistedTaskInfo implements Writeable, ToXContent {
    private final TaskInfo task;
    @Nullable
    private final BytesReference error;
    @Nullable
    private final BytesReference result;

    /**
     * Construct a {@linkplain PersistedTaskInfo} for a running task.
     */
    public PersistedTaskInfo(TaskInfo task) {
        this(task, null, null);
    }

    /**
     * Construct a {@linkplain PersistedTaskInfo} for a task that completed with an error.
     */
    public PersistedTaskInfo(TaskInfo task, Throwable error) throws IOException {
        this(task, toXContent(error), null);
    }

    /**
     * Construct a {@linkplain PersistedTaskInfo} for a task that completed successfully.
     */
    public PersistedTaskInfo(TaskInfo task, ToXContent result) throws IOException {
        this(task, null, toXContent(result));
    }

    private PersistedTaskInfo(TaskInfo task, @Nullable BytesReference error, @Nullable BytesReference result) {
        this.task = requireNonNull(task, "task is required");
        this.error = error;
        this.result = result;
    }

    /**
     * Read from a stream.
     */
    public PersistedTaskInfo(StreamInput in) throws IOException {
        task = new TaskInfo(in);
        error = in.readOptionalBytesReference();
        result = in.readOptionalBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        task.writeTo(out);
        out.writeOptionalBytesReference(error);
        out.writeOptionalBytesReference(result);
    }

    /**
     * Get the task that this wraps.
     */
    public TaskInfo getTask() {
        return task;
    }

    /**
     * Get the error that finished this task. Will return null if the task didn't finish with an error or it hasn't yet finished.
     */
    public BytesReference getError() {
        return error;
    }

    /**
     * Convert {@link #getError()} from XContent to a Map for easy processing. Will return null if the task didn't finish with an error or
     * hasn't yet finished.
     */
    public Map<String, Object> getErrorAsMap() {
        if (error == null) {
            return null;
        }
        return convertToMap(error, false).v2();
    }

    /**
     * Get the result that this task finished with. Will return null if the task was finished by an error or it hasn't yet finished.
     */
    public BytesReference getResult() {
        return result;
    }

    /**
     * Convert {@link #getResult()} from XContent to a Map for easy processing. Will return null if the task was finished with an error or
     * hasn't yet finished.
     */
    public Map<String, Object> getResultAsMap() {
        if (result == null) {
            return null;
        }
        return convertToMap(result, false).v2();
    }

    /**
     * Was the task completed before returned?
     */
    public boolean isCompleted() {
        return error != null || result != null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        return builder.endObject();
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("task", task);
        if (error != null) {
            XContentHelper.writeRawField("error", error, builder, params);
        }
        if (result != null) {
            XContentHelper.writeRawField("result", result, builder, params);
        }
        return builder;
    }

    public static final ConstructingObjectParser<PersistedTaskInfo, ParseFieldMatcherSupplier> PARSER = new ConstructingObjectParser<>(
            "persisted_task_info", a -> new PersistedTaskInfo((TaskInfo) a[0], (BytesReference) a[1], (BytesReference) a[2]));
    static {
        PARSER.declareObject(constructorArg(), TaskInfo.PARSER, new ParseField("task"));
        PARSER.declareRawObject(optionalConstructorArg(), new ParseField("error"));
        PARSER.declareRawObject(optionalConstructorArg(), new ParseField("result"));
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != PersistedTaskInfo.class) {
            return false;
        }
        PersistedTaskInfo other = (PersistedTaskInfo) obj;
        /*
         * Equality of error and result is done by converting them to a map first. Not efficient but ignores field order and spacing
         * differences so perfect for testing.
         */
        return Objects.equals(task, other.task)
                && Objects.equals(getErrorAsMap(), other.getErrorAsMap())
                && Objects.equals(getResultAsMap(), other.getResultAsMap());
    }

    @Override
    public int hashCode() {
        /*
         * Hashing of error and result is done by converting them to a map first. Not efficient but ignores field order and spacing
         * differences so perfect for testing.
         */
        return Objects.hash(task, getErrorAsMap(), getResultAsMap());
    }

    private static BytesReference toXContent(ToXContent result) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            // Elasticsearch's Response object never emit starting or ending objects. Most other implementers of ToXContent do....
            builder.startObject();
            result.toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return builder.bytes();
        }
    }

    private static BytesReference toXContent(Throwable error) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            builder.startObject();
            ElasticsearchException.toXContent(builder, ToXContent.EMPTY_PARAMS, error);
            builder.endObject();
            return builder.bytes(); 
        }
    }
}
