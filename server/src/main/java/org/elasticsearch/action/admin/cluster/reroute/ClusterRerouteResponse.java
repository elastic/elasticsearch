/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.IsAcknowledgedSupplier;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;

import static org.elasticsearch.action.support.master.AcknowledgedResponse.ACKNOWLEDGED_KEY;

/**
 * Response returned after a cluster reroute request
 */
public class ClusterRerouteResponse extends ActionResponse implements IsAcknowledgedSupplier, ChunkedToXContentObject {

    private final RoutingExplanations explanations;
    private final boolean acknowledged;

    ClusterRerouteResponse(StreamInput in) throws IOException {
        super(in);
        acknowledged = in.readBoolean();
        explanations = RoutingExplanations.readFrom(in);
    }

    ClusterRerouteResponse(boolean acknowledged, RoutingExplanations explanations) {
        this.acknowledged = acknowledged;
        this.explanations = explanations;
    }

    public RoutingExplanations getExplanations() {
        return this.explanations;
    }

    @Override
    public final boolean isAcknowledged() {
        return acknowledged;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
        RoutingExplanations.writeTo(explanations, out);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return toXContentChunkedV7(outerParams);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunkedV7(ToXContent.Params outerParams) {
        return Iterators.concat(
            Iterators.single((builder, params) -> builder.startObject().field(ACKNOWLEDGED_KEY, isAcknowledged())),
            Iterators.single((builder, params) -> {
                if (params.paramAsBoolean("explain", false)) {
                    explanations.toXContent(builder, params);
                }
                builder.endObject();
                return builder;
            })
        );
    }
}
