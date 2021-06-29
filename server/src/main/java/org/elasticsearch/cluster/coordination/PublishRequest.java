/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;

import java.util.Objects;

/**
 * Request which is used by the master node to publish cluster state changes.
 * Actual serialization of this request is done by {@link PublicationTransportHandler}
 */
public class PublishRequest {

    private final ClusterState acceptedState;

    public PublishRequest(ClusterState acceptedState) {
        this.acceptedState = acceptedState;
    }

    public ClusterState getAcceptedState() {
        return acceptedState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof PublishRequest) == false) return false;

        PublishRequest that = (PublishRequest) o;

        return acceptedState.term() == that.acceptedState.term() &&
            acceptedState.version() == that.acceptedState.version();
    }

    @Override
    public int hashCode() {
        return Objects.hash(acceptedState.term(), acceptedState.version());
    }

    @Override
    public String toString() {
        return "PublishRequest{term=" + acceptedState.term()
            + ", version=" + acceptedState.version()
            + ", state=" + acceptedState + '}';
    }
}
