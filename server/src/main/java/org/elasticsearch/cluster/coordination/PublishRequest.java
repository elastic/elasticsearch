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
        if (!(o instanceof PublishRequest)) return false;

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
