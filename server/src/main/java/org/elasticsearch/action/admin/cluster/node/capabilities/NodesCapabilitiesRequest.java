/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.capabilities;

import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;

import java.util.Set;

public class NodesCapabilitiesRequest extends BaseNodesRequest {

    private RestRequest.Method method = RestRequest.Method.GET;
    private String path = "/";
    private Set<String> parameters = Set.of();
    private Set<String> capabilities = Set.of();
    private RestApiVersion restApiVersion = RestApiVersion.current();

    public NodesCapabilitiesRequest() {
        // send to all nodes
        super(Strings.EMPTY_ARRAY);
    }

    public NodesCapabilitiesRequest(String nodeId) {
        // only send to this node (the local node)
        super(new String[] { nodeId });
    }

    public NodesCapabilitiesRequest path(String path) {
        this.path = path;
        return this;
    }

    public String path() {
        return path;
    }

    public NodesCapabilitiesRequest method(RestRequest.Method method) {
        this.method = method;
        return this;
    }

    public RestRequest.Method method() {
        return method;
    }

    public NodesCapabilitiesRequest parameters(String... parameters) {
        this.parameters = Set.of(parameters);
        return this;
    }

    public Set<String> parameters() {
        return parameters;
    }

    public NodesCapabilitiesRequest capabilities(String... capabilities) {
        this.capabilities = Set.of(capabilities);
        return this;
    }

    public Set<String> capabilities() {
        return capabilities;
    }

    public NodesCapabilitiesRequest restApiVersion(RestApiVersion restApiVersion) {
        this.restApiVersion = restApiVersion;
        return this;
    }

    public RestApiVersion restApiVersion() {
        return restApiVersion;
    }
}
