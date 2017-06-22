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

import org.elasticsearch.cluster.routing.allocation.command.AllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

/**
 * Class encapsulating the explanation for a single {@link AllocationCommand}
 * taken from the Deciders
 */
public class RerouteExplanation implements ToXContent {

    private AllocationCommand command;
    private Decision decisions;
    private List<String> warnings;

    public RerouteExplanation(AllocationCommand command, Decision decisions) {
        this(command, decisions, emptyList());
    }

    public RerouteExplanation(AllocationCommand command, Decision decisions, List<String> warnings) {
        this.command = command;
        this.decisions = decisions;
        this.warnings = unmodifiableList(warnings);
    }

    public AllocationCommand command() {
        return this.command;
    }

    public Decision decisions() {
        return this.decisions;
    }

    public List<String> warnings() { return this.warnings; }

    public boolean hasWarnings() {
        return warnings().size() > 0;
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
        builder.field("warnings", warnings);
        builder.endObject();
        return builder;
    }
}
