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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Class used to encapsulate a number of {@link RerouteExplanation}
 * explanations.
 */
public class RoutingExplanations implements ToXContent {
    private final List<RerouteExplanation> explanations;

    public RoutingExplanations() {
        this.explanations = new ArrayList<>();
    }

    public RoutingExplanations add(RerouteExplanation explanation) {
        this.explanations.add(explanation);
        return this;
    }

    public List<RerouteExplanation> explanations() {
        return this.explanations;
    }

    /**
     * Read in a RoutingExplanations object
     */
    public static RoutingExplanations readFrom(StreamInput in) throws IOException {
        int exCount = in.readVInt();
        RoutingExplanations exp = new RoutingExplanations();
        for (int i = 0; i < exCount; i++) {
            RerouteExplanation explanation = RerouteExplanation.readFrom(in);
            exp.add(explanation);
        }
        return exp;
    }

    /**
     * Write the RoutingExplanations object
     */
    public static void writeTo(RoutingExplanations explanations, StreamOutput out) throws IOException {
        out.writeVInt(explanations.explanations.size());
        for (RerouteExplanation explanation : explanations.explanations) {
            RerouteExplanation.writeTo(explanation, out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("explanations");
        for (RerouteExplanation explanation : explanations) {
            explanation.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
