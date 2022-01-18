/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Class encapsulating the explanation for a single {@link AllocationCommand}
 * taken from the Deciders
 */
public class RerouteExplanation implements ToXContentObject {

    private AllocationCommand command;
    private Decision decisions;

    public RerouteExplanation(AllocationCommand command, Decision decisions) {
        this.command = command;
        this.decisions = decisions;
    }

    public AllocationCommand command() {
        return this.command;
    }

    public Decision decisions() {
        return this.decisions;
    }

    public static RerouteExplanation readFrom(StreamInput in) throws IOException {
        AllocationCommand command = in.readNamedWriteable(AllocationCommand.class);
        Decision decisions = Decision.readFrom(in);
        return new RerouteExplanation(command, decisions);
    }

    public static void writeTo(RerouteExplanation explanation, StreamOutput out) throws IOException {
        out.writeNamedWriteable(explanation.command);
        explanation.decisions.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("command", command.name());
        builder.field("parameters", command);
        builder.startArray("decisions");
        decisions.toXContent(builder, params);
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
