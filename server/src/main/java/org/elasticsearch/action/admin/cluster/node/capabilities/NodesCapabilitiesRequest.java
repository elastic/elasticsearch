/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.capabilities;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.RestRequest;

import java.io.IOException;
import java.util.Set;

public class NodesCapabilitiesRequest extends BaseNodesRequest<NodesCapabilitiesRequest> {

    private RestRequest.Method method = RestRequest.Method.GET;
    private String path = "/";
    private Set<String> parameters = Set.of();
    private Set<String> features = Set.of();

    public NodesCapabilitiesRequest(StreamInput in) throws IOException {
        super(in);

        method = in.readEnum(RestRequest.Method.class);
        path = in.readString();
        parameters = in.readCollectionAsImmutableSet(StreamInput::readString);
        features = in.readCollectionAsImmutableSet(StreamInput::readString);
    }

    public NodesCapabilitiesRequest(String... nodeIds) {
        super(nodeIds);
    }

    public NodesCapabilitiesRequest path(String path) {
        this.path = path;
        return this;
    }

    public NodesCapabilitiesRequest method(RestRequest.Method method) {
        this.method = method;
        return this;
    }

    public RestRequest.Method method() {
        return method;
    }

    public String path() {
        return path;
    }

    public NodesCapabilitiesRequest parameters(Set<String> parameters) {
        this.parameters = Set.copyOf(parameters);
        return this;
    }

    public Set<String> parameters() {
        return parameters;
    }

    public NodesCapabilitiesRequest features(Set<String> features) {
        this.features = Set.copyOf(features);
        return this;
    }

    public Set<String> features() {
        return features;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeEnum(method);
        out.writeString(path);
        out.writeCollection(parameters, StreamOutput::writeString);
        out.writeCollection(features, StreamOutput::writeString);
    }
}
