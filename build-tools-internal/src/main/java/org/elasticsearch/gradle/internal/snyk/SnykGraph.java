/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk;

import com.google.common.collect.Maps;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SnykGraph {

    Map<String, Object> nodes = Maps.newLinkedHashMap();

    String rootId;

    public SnykGraph(String rootId) {
        this.rootId = rootId;
    }

    public Map<String, Object> setNode(String key, Map<String, Object> value) {
        if (key == null) {
            return null;
        }
        if (nodes.containsKey(key)) {
            return (Map<String, Object>) this.nodes.get(key);
        }
        if (value == null) {
            return null;
        }
        Map<String, Object> vertex = Maps.newLinkedHashMapWithExpectedSize(3);
        vertex.put("name", value.get("name"));
        vertex.put("version", value.get("version"));
        vertex.put("parentIds", new LinkedHashSet<>());
        return (Map<String, Object>) nodes.put(key, vertex);

    }

    public Map<String, Object> getNodes() {
        return nodes;
    }

    public String getRootId() {
        return rootId;
    }

    public void setEdge(String parentId, String childId) {
        if (parentId == null || childId == null || parentId == childId) {
            return;
        }
        // root-node will be the graphlib root that first-level deps will be attached to
        if (parentId != this.rootId) {
            Map<String, Object> parentNode = this.setNode(parentId, null);
            if (parentNode == null) {
                return;
            }
        }
        var childNode = this.setNode(childId, null);
        if (childNode == null || ((Set) childNode.get("parentIds")).contains(parentId)) {
            return;
        }
        ((Set) childNode.get("parentIds")).add(parentId);
    }

    @Override
    public String toString() {
        String collect = nodes.entrySet()
            .stream()
            .map(stringObjectEntry -> stringObjectEntry.getKey() + ": " + stringObjectEntry.getValue())
            .collect(Collectors.joining("\n"));

        return "SnykGraph{" + "nodes=\n" + collect + ", rootId='" + rootId + '\'' + '}';
    }

    public void toSnykApiGraph() {
        Set<Map> pkgs = new LinkedHashSet();
        Set<Map> nodes = new LinkedHashSet();
        getNodes().forEach((nodeId, nodeData) -> {
            LinkedHashMap<String, Object> pkg = new LinkedHashMap<>();
            LinkedHashMap<String, Object> pkgInfo = new LinkedHashMap<>();
            pkgInfo.put("name", ((Map) nodeData).get("name"));
            pkgInfo.put("version", ((Map) nodeData).get("version"));
            pkg.put("id", nodeId);
            pkg.put("info", pkgInfo);
            pkgs.add(pkg);

            LinkedHashMap<String, Object> node = new LinkedHashMap<>();
            node.put("nodeId", nodeId);
            node.put("pkgId", nodeId);

            LinkedHashMap<String, Object> deps = new LinkedHashMap<>();
            Object parentIds = ((Map<?, ?>) nodeData).get("parentIds");

        });
    }
}
