/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class MockAction implements LifecycleAction {
    public static final ParseField COMPLETED_FIELD = new ParseField("completed");
    public static final ParseField EXECUTED_COUNT_FIELD = new ParseField("executed_count");
    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final String NAME = "TEST_ACTION";
    private SetOnce<Boolean> completed = new SetOnce<>();
    private final AtomicLong executedCount;
    private Exception exceptionToThrow = null;
    private boolean completeOnExecute = false;

    private static final ConstructingObjectParser<MockAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new MockAction((Boolean) a[0], (Long) a[1]));
    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), COMPLETED_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), EXECUTED_COUNT_FIELD);
    }

    public static MockAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public MockAction() {
        this(null, 0);
    }

    MockAction(Boolean completed, long executedCount) {
        if (completed != null) {
            this.completed.set(completed);
        }
        this.executedCount = new AtomicLong(executedCount);
    }

    public MockAction(StreamInput in) throws IOException {
        Boolean executed = in.readOptionalBoolean();
        if (executed != null) {
            this.completed.set(executed);
        }
        this.executedCount = new AtomicLong(in.readLong());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (completed.get() != null) {
            builder.field(COMPLETED_FIELD.getPreferredName(), completed.get());
        }
        builder.field(EXECUTED_COUNT_FIELD.getPreferredName(), executedCount.get());
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public void setCompleteOnExecute(boolean completeOnExecute) {
        this.completeOnExecute = completeOnExecute;
    }

    public void setExceptionToThrow(Exception exceptionToThrow) {
        this.exceptionToThrow = exceptionToThrow;
    }

    public boolean wasCompleted() {
        return completed.get() != null && completed.get();
    }

    public void resetCompleted() {
        completed = new SetOnce<>();
    }

    public long getExecutedCount() {
        return executedCount.get();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(completed.get());
        out.writeLong(executedCount.get());
    }

    @Override
    public void execute(Index index, Client client, ClusterService clusterService, Listener listener) {
        executedCount.incrementAndGet();
        if (exceptionToThrow == null) {
            if (completeOnExecute) {
                completed.set(true);
            }
            listener.onSuccess(completeOnExecute);
        } else {
            listener.onFailure(exceptionToThrow);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(completed.get(), executedCount.get());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        MockAction other = (MockAction) obj;
        return Objects.equals(completed.get(), other.completed.get()) &&
                Objects.equals(executedCount.get(), other.executedCount.get());
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}