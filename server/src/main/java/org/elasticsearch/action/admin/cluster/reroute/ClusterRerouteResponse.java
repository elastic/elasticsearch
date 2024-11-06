/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.reroute;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.master.IsAcknowledgedSupplier;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.RoutingExplanations;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

import static org.elasticsearch.action.support.master.AcknowledgedResponse.ACKNOWLEDGED_KEY;

/**
 * Response returned after a cluster reroute request
 */
public class ClusterRerouteResponse extends ActionResponse implements IsAcknowledgedSupplier, ChunkedToXContentObject {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestSearchAction.class);
    public static final String STATE_FIELD_DEPRECATION_MESSAGE = "The [state] field in the response to the reroute API is deprecated "
        + "and will be removed in a future version. Specify ?metric=none to adopt the future behaviour.";

    /**
     * To be removed when REST compatibility with {@link org.elasticsearch.Version#V_8_6_0} / {@link RestApiVersion#V_8} no longer needed
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION)  // to remove entirely
    private final ClusterState state;
    private final RoutingExplanations explanations;
    private final boolean acknowledged;

    ClusterRerouteResponse(StreamInput in) throws IOException {
        super(in);
        acknowledged = in.readBoolean();
        state = ClusterState.readFrom(in, null);
        explanations = RoutingExplanations.readFrom(in);
    }

    ClusterRerouteResponse(boolean acknowledged, ClusterState state, RoutingExplanations explanations) {
        this.acknowledged = acknowledged;
        this.state = state;
        this.explanations = explanations;
    }

    /**
     * Returns the cluster state resulted from the cluster reroute request execution
     */
    public ClusterState getState() {
        return this.state;
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
        state.writeTo(out);
        RoutingExplanations.writeTo(explanations, out);
    }

    private static boolean emitState(ToXContent.Params params) {
        return Objects.equals(params.param("metric"), "none") == false;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        if (emitState(outerParams)) {
            deprecationLogger.critical(DeprecationCategory.API, "reroute_cluster_state", STATE_FIELD_DEPRECATION_MESSAGE);
        }
        return ChunkedToXContent.builder(outerParams).object(b -> {
            b.field(ACKNOWLEDGED_KEY, isAcknowledged());
            if (emitState(outerParams)) {
                b.xContentObject("state", state);
            }
            if (outerParams.paramAsBoolean("explain", false)) {
                b.append(explanations);
            }
        });
    }
}
