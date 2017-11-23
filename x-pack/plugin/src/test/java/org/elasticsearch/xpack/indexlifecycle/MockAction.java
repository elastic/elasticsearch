/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.Objects;

public class MockAction extends LifecycleAction {
    public static final ParseField EXECUTED_FIELD = new ParseField("executed");
    public static final ParseField NAME_FIELD = new ParseField("name");
    public static final String NAME = "TEST_ACTION";
    private final SetOnce<Boolean> executed = new SetOnce<>();

    private static final ConstructingObjectParser<MockAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
            a -> new MockAction((Boolean) a[1]));
    static {
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), EXECUTED_FIELD);
    }

    public static MockAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public MockAction() {
    }

    private MockAction(Boolean executed) {
        if (executed != null) {
            this.executed.set(executed);
        }
    }

    public MockAction(StreamInput in) throws IOException {
        Boolean executed = in.readOptionalBoolean();
        if (executed != null) {
            this.executed.set(executed);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (executed.get() != null) {
            builder.field("executed", executed.get());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public Boolean wasExecuted() {
        return executed.get() != null && executed.get();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalBoolean(executed.get());
    }

    @Override
    protected void execute(Index index, Client client) {
        executed.set(true);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executed.get());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() == getClass()) {
            return false;
        }
        MockAction other = (MockAction) obj;
        return Objects.equals(executed.get(), other.executed.get());
    }

}