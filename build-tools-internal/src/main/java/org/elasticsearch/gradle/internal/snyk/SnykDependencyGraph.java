/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class SnykDependencyGraph {
    static private final String schemaVersion = "1.2.0";
    private final Map<String, Object> graph;
    private final Set<SnykDependencyPkg> pkgs;
    private final Map<String, String> pkgManager;

    public SnykDependencyGraph(String gradleVersion, Set<SnykDependencyNode> nodes, Set<SnykDependencyPkg> pkgs) {
        this.pkgs = pkgs;
        this.graph = new HashMap();
        graph.put("rootNodeId", "root-node");
        graph.put("nodes", nodes);
        this.pkgManager = Map.of("name", "gradle", "version", gradleVersion);
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public Map<String, String> getPkgManager() {
        return pkgManager;
    }

    public Map<String, Object> getGraph() {
        return graph;
    }

    public Set<SnykDependencyPkg> getPkgs() {
        return pkgs;
    }

    static class SnykDependencyNode {
        private String nodeId;
        private String pkgId;
        private Set<Map<String, String>> deps = new LinkedHashSet<>();

        SnykDependencyNode(String nodeId, String pkgId) {
            this.nodeId = nodeId;
            this.pkgId = pkgId;
        }

        public void addDep(String pkg) {
            deps.add(Map.of("nodeId", pkg));
        }

        public String getNodeId() {
            return nodeId;
        }

        public String getPkgId() {
            return pkgId;
        }

        public Set<Map<String, String>> getDeps() {
            return deps;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SnykDependencyNode that = (SnykDependencyNode) o;
            return Objects.equals(nodeId, that.nodeId) && Objects.equals(pkgId, that.pkgId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(nodeId, pkgId);
        }

        @Override
        public String toString() {
            return "SnykDependencyNode{" + "nodeId='" + nodeId + '\'' + ", pkgId='" + pkgId + '\'' + '}';
        }
    }

    static class SnykDependencyPkg {
        private final String id;
        private final Map<String, String> info;

        SnykDependencyPkg(String pkgId) {
            id = pkgId;
            String name = id.substring(0, id.indexOf('@'));
            String version = id.substring(id.indexOf('@') + 1);
            info = Map.of("name", name, "version", version);
        }

        public String getId() {
            return id;
        }

        public Map<String, String> getInfo() {
            return info;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SnykDependencyPkg that = (SnykDependencyPkg) o;
            return Objects.equals(id, that.id) && Objects.equals(info, that.info);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, info);
        }
    }

}
