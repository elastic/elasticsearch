/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.backwards;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

final class MixedClusterTestNodes {
    private final Map<String, MixedClusterTestNode> nodesById;
    private final String bwcNodesVersion;

    private MixedClusterTestNodes(String bwcNodesVersion, Map<String, MixedClusterTestNode> nodesById) {
        this.bwcNodesVersion = bwcNodesVersion;
        this.nodesById = nodesById;
    }

    public List<MixedClusterTestNode> getNewNodes() {
        return nodesById.values().stream().filter(n -> n.version().equals(bwcNodesVersion) == false).collect(Collectors.toList());
    }

    public List<MixedClusterTestNode> getBWCNodes() {
        return nodesById.values().stream().filter(n -> n.version().equals(bwcNodesVersion)).collect(Collectors.toList());
    }

    public MixedClusterTestNode getSafe(String id) {
        MixedClusterTestNode node = nodesById.get(id);
        if (node == null) {
            throw new IllegalArgumentException("node with id [" + id + "] not found");
        }
        return node;
    }

    static MixedClusterTestNodes buildNodes(RestClient client, String bwcNodesVersion) throws IOException {
        Response response = client.performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");

        Map<String, MixedClusterTestNode> nodesById = new HashMap<>();
        for (var id : nodesAsMap.keySet()) {
            nodesById.put(
                id,
                new MixedClusterTestNode(
                    id,
                    objectPath.evaluate("nodes." + id + ".name"),
                    objectPath.evaluate("nodes." + id + ".version"),
                    HttpHost.create(objectPath.evaluate("nodes." + id + ".http.publish_address"))
                )
            );
        }
        return new MixedClusterTestNodes(bwcNodesVersion, Collections.unmodifiableMap(nodesById));
    }

    public int size() {
        return nodesById.size();
    }
}
