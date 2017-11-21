/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.List;

public class Phase implements ToXContentObject, Writeable {

    public static final ParseField AFTER_FIELD = new ParseField("after");
    public static final ParseField ACTIONS_FIELD = new ParseField("actions");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<Phase, Tuple<String, NamedXContentRegistry>> PARSER = new ConstructingObjectParser<>(
            "phase", false, (a, c) -> new Phase(c.v1(), (TimeValue) a[0], (List<LifecycleAction>) a[1]));
    static {
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.text(), AFTER_FIELD.getPreferredName()), AFTER_FIELD, ValueType.VALUE);
        PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(),
                (p, c, n) -> c.v2().parseNamedObject(LifecycleAction.class, n, p, c.v2()), v -> {
                    throw new IllegalArgumentException("ordered " + ACTIONS_FIELD.getPreferredName() + " are not supported");
                }, ACTIONS_FIELD);
    }

    public static Phase parse(XContentParser parser, Tuple<String, NamedXContentRegistry> context) {
        return PARSER.apply(parser, context);
    }

    private String name;
    private List<LifecycleAction> actions;
    private TimeValue after;

    public Phase(String name, TimeValue after, List<LifecycleAction> actions) {
        this.name = name;
        this.after = after;
        this.actions = actions;
    }

    public Phase(StreamInput in) throws IOException {
        this.name = in.readString();
        this.after = new TimeValue(in);
        this.actions = in.readNamedWriteableList(LifecycleAction.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        after.writeTo(out);
        out.writeNamedWriteableList(actions);
    }

    public TimeValue getAfter() {
        return after;
    }

    public String getName() {
        return name;
    }

    public List<LifecycleAction> getActions() {
        return actions;
    }

    protected void performActions(Client client, Index index) {
        for (LifecycleAction action : actions) {
            action.execute(client, index);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(AFTER_FIELD.getPreferredName(), after);
        builder.startObject(ACTIONS_FIELD.getPreferredName());
        for (LifecycleAction action : actions) {
            builder.field(action.getWriteableName(), action);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

}
