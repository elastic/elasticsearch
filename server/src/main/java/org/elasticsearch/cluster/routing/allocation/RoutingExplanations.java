/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Class used to encapsulate a number of {@link RerouteExplanation}
 * explanations.
 */
public class RoutingExplanations implements ToXContentFragment {
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
     * Provides feedback from commands with a YES decision that should be displayed to the user after the command has been applied
     */
    public List<String> getYesDecisionMessages() {
        return explanations().stream()
            .filter(explanation -> explanation.decisions().type().equals(Decision.Type.YES))
            .map(explanation -> explanation.command().getMessage())
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
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
