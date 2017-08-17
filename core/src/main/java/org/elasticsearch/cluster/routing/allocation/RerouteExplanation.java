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

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Optional;

/**
 * Class encapsulating the explanation for a single {@link AllocationCommand}
 * taken from the Deciders
 */
public class RerouteExplanation implements ToXContentObject {

    private AllocationCommand command;
    private Decision decisions;
    private String message;

    public RerouteExplanation(AllocationCommand command, Decision decisions, String message) {
        this.command = command;
        this.decisions = decisions;
        this.message = message;
    }

    public RerouteExplanation(AllocationCommand command, Decision decisions) {
        this(command, decisions, null);
    }

    public AllocationCommand command() {
        return this.command;
    }

    public Decision decisions() {
        return this.decisions;
    }

    /**
     * A message that may be displayed to the user after this explanation's command has been applied
     */
    public Optional<String> message() {
        return Optional.ofNullable(message);
    }


    public static RerouteExplanation readFrom(StreamInput in) throws IOException {
        AllocationCommand command = in.readNamedWriteable(AllocationCommand.class);
        Decision decisions = Decision.readFrom(in);

        String message;
        if (in.getVersion().onOrAfter(Version.V_6_1_0)) {
            message = in.readOptionalString();
        } else {
            message = null;
        }

        return new RerouteExplanation(command, decisions, message);
    }

    public static void writeTo(RerouteExplanation explanation, StreamOutput out) throws IOException {
        out.writeNamedWriteable(explanation.command);
        explanation.decisions.writeTo(out);
        if (out.getVersion().onOrAfter(Version.V_6_1_0)) {
            out.writeOptionalString(explanation.message);
        }
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
