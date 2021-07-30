/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.tasks;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Requests;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.InstantiatingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.common.xcontent.XContentHelper.convertToMap;

/**
 * Information about a running task or a task that stored its result. Running tasks just have a {@link #getTask()} while
 * tasks with stored result will have either a {@link #getError()} or {@link #getResponse()}.
 */
public final class TaskResult implements Writeable, ToXContentObject {
    private final boolean completed;
    private final TaskInfo task;
    @Nullable
    private final BytesReference error;
    @Nullable
    private final BytesReference response;

    /**
     * Construct a {@linkplain TaskResult} for a task for which we don't have a result or error. That usually means that the task
     * is incomplete, but it could also mean that we waited for the task to complete but it didn't save any error information.
     */
    public TaskResult(boolean completed, TaskInfo task) {
        this(completed, task, null, null);
    }

    /**
     * Construct a {@linkplain TaskResult} for a task that completed with an error.
     */
    public TaskResult(TaskInfo task, Exception error) throws IOException {
        this(true, task, toXContent(error), null);
    }

    /**
     * Construct a {@linkplain TaskResult} for a task that completed successfully.
     */
    public TaskResult(TaskInfo task, ToXContent response) throws IOException {
        this(true, task, null, XContentHelper.toXContent(response, Requests.INDEX_CONTENT_TYPE, true));
    }

    public TaskResult(boolean completed, TaskInfo task, @Nullable BytesReference error, @Nullable BytesReference result) {
        this.completed = completed;
        this.task = requireNonNull(task, "task is required");
        this.error = error;
        this.response = result;
    }

    /**
     * Read from a stream.
     */
    public TaskResult(StreamInput in) throws IOException {
        completed = in.readBoolean();
        task = new TaskInfo(in);
        error = in.readOptionalBytesReference();
        response = in.readOptionalBytesReference();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(completed);
        task.writeTo(out);
        out.writeOptionalBytesReference(error);
        out.writeOptionalBytesReference(response);
    }

    /**
     * Get the task that this wraps.
     */
    public TaskInfo getTask() {
        return task;
    }

    /**
     * Get the error that finished this task. Will return null if the task didn't finish with an error, it hasn't yet finished, or didn't
     * store its result.
     */
    public BytesReference getError() {
        return error;
    }

    /**
     * Convert {@link #getError()} from XContent to a Map for easy processing. Will return an empty map if the task didn't finish with an
     * error, hasn't yet finished, or didn't store its result.
     */
    public Map<String, Object> getErrorAsMap() {
        if (error == null) {
            return emptyMap();
        }
        return convertToMap(error, false).v2();
    }

    /**
     * Get the response that this task finished with. Will return null if the task was finished by an error, it hasn't yet finished, or
     * didn't store its result.
     */
    public BytesReference getResponse() {
        return response;
    }

    /**
     * Convert {@link #getResponse()} from XContent to a Map for easy processing. Will return an empty map if the task was finished with an
     * error, hasn't yet finished, or didn't store its result.
     */
    public Map<String, Object> getResponseAsMap() {
        if (response == null) {
            return emptyMap();
        }
        return convertToMap(response, false).v2();
    }

    public boolean isCompleted() {
        return completed;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        return builder.endObject();
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("completed", completed);
        builder.startObject("task");
        task.toXContent(builder, params);
        builder.endObject();
        if (error != null) {
            XContentHelper.writeRawField("error", error, builder, params);
        }
        if (response != null) {
            XContentHelper.writeRawField("response", response, builder, params);
        }
        return builder;
    }

    public static final InstantiatingObjectParser<TaskResult, Void> PARSER;

    static {
        InstantiatingObjectParser.Builder<TaskResult, Void> parser = InstantiatingObjectParser.builder(
            "stored_task_result", true, TaskResult.class);
        parser.declareBoolean(constructorArg(), new ParseField("completed"));
        parser.declareObject(constructorArg(), TaskInfo.PARSER, new ParseField("task"));
        ObjectParserHelper<TaskResult, Void> parserHelper = new ObjectParserHelper<>();
        parserHelper.declareRawObject(parser, optionalConstructorArg(), new ParseField("error"));
        parserHelper.declareRawObject(parser, optionalConstructorArg(), new ParseField("response"));
        PARSER = parser.build();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    // Implements equals and hashcode for testing
    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != TaskResult.class) {
            return false;
        }
        TaskResult other = (TaskResult) obj;
        /*
         * Equality of error and result is done by converting them to a map first. Not efficient but ignores field order and spacing
         * differences so perfect for testing.
         */
        return Objects.equals(completed, other.completed)
                && Objects.equals(task, other.task)
                && Objects.equals(getErrorAsMap(), other.getErrorAsMap())
                && Objects.equals(getResponseAsMap(), other.getResponseAsMap());
    }

    @Override
    public int hashCode() {
        /*
         * Hashing of error and result is done by converting them to a map first. Not efficient but ignores field order and spacing
         * differences so perfect for testing.
         */
        return Objects.hash(completed, task, getErrorAsMap(), getResponseAsMap());
    }

    private static BytesReference toXContent(Exception error) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE)) {
            builder.startObject();
            ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, error);
            builder.endObject();
            return BytesReference.bytes(builder);
        }
    }
}
