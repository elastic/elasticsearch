/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.history;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;

import static org.elasticsearch.ElasticsearchException.REST_EXCEPTION_SKIP_STACK_TRACE;

/**
 * The {@link ILMHistoryItem} class encapsulates the state of an index at a point in time. It should
 * be constructed when an index has transitioned into a new step. Construction is done through the
 * {@link #success(String, String, long, Long, LifecycleExecutionState)} and
 * {@link #failure(String, String, long, Long, LifecycleExecutionState, Exception)} methods.
 */
public class ILMHistoryItem implements ToXContentObject {
    private static final ParseField INDEX = new ParseField("index");
    private static final ParseField POLICY = new ParseField("policy");
    private static final ParseField TIMESTAMP = new ParseField("@timestamp");
    private static final ParseField INDEX_AGE = new ParseField("index_age");
    private static final ParseField SUCCESS = new ParseField("success");
    private static final ParseField EXECUTION_STATE = new ParseField("state");
    private static final ParseField ERROR = new ParseField("error_details");

    private final String index;
    private final String policyId;
    private final long timestamp;
    @Nullable
    private final Long indexAge;
    private final boolean success;
    @Nullable
    private final LifecycleExecutionState executionState;
    @Nullable
    private final String errorDetails;

    private ILMHistoryItem(String index, String policyId, long timestamp, @Nullable Long indexAge, boolean success,
                           @Nullable LifecycleExecutionState executionState, @Nullable String errorDetails) {
        this.index = index;
        this.policyId = policyId;
        this.timestamp = timestamp;
        this.indexAge = indexAge;
        this.success = success;
        this.executionState = executionState;
        this.errorDetails = errorDetails;
    }

    public static ILMHistoryItem success(String index, String policyId, long timestamp, @Nullable Long indexAge,
                                         @Nullable LifecycleExecutionState executionState) {
        return new ILMHistoryItem(index, policyId, timestamp, indexAge, true, executionState, null);
    }

    public static ILMHistoryItem failure(String index, String policyId, long timestamp, @Nullable Long indexAge,
                                         @Nullable LifecycleExecutionState executionState, Exception error) {
        Objects.requireNonNull(error, "ILM failures require an attached exception");
        return new ILMHistoryItem(index, policyId, timestamp, indexAge, false, executionState, exceptionToString(error));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX.getPreferredName(), index);
        builder.field(POLICY.getPreferredName(), policyId);
        builder.field(TIMESTAMP.getPreferredName(), timestamp);
        if (indexAge != null) {
            builder.field(INDEX_AGE.getPreferredName(), indexAge);
        }
        builder.field(SUCCESS.getPreferredName(), success);
        if (executionState != null) {
            builder.field(EXECUTION_STATE.getPreferredName(), executionState.asMap());
        }
        if (errorDetails != null) {
            builder.field(ERROR.getPreferredName(), errorDetails);
        }
        builder.endObject();
        return builder;
    }

    private static String exceptionToString(Exception exception) {
        Params stacktraceParams = new MapParams(Collections.singletonMap(REST_EXCEPTION_SKIP_STACK_TRACE, "false"));
        String exceptionString;
        try (XContentBuilder causeXContentBuilder = JsonXContent.contentBuilder()) {
            causeXContentBuilder.startObject();
            ElasticsearchException.generateThrowableXContent(causeXContentBuilder, stacktraceParams, exception);
            causeXContentBuilder.endObject();
            exceptionString = BytesReference.bytes(causeXContentBuilder).utf8ToString();
        } catch (IOException e) {
            // In the unlikely case that we cannot generate an exception string,
            // try the best way can to encapsulate the error(s) with at least
            // the message
            exceptionString = "unable to generate the ILM error details due to: " + e.getMessage() +
                "; the ILM error was: " + exception.getMessage();
        }
        return exceptionString;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
