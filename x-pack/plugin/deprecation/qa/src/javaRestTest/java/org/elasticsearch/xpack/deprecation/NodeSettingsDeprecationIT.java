/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.deprecation;

import org.elasticsearch.client.Request;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

/**
 * Checks the node settings issues reported by the deprecation info API for a cluster with a default configuration.
 */
public class NodeSettingsDeprecationIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .module("x-pack-deprecation")
        .module("x-pack-stack")
        .module("x-pack-ilm")
        .module("x-pack-ml")
        .module("mapper-extras")
        .module("wildcard")
        .module("ingest-common")
        .module("constant-keyword")
        .module("transform")
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testNodeSettingsIssues() throws Exception {
        final var nodesInfo = assertOKAndCreateObjectPath(adminClient().performRequest(new Request("GET", "/_nodes/jvm")));
        final Map<String, Object> nodes = nodesInfo.evaluate("nodes");
        assertThat(nodes.size(), equalTo(1));
        final String nodeId = nodes.keySet().iterator().next();
        final String javaVersion = nodesInfo.evaluate("nodes." + nodeId + ".jvm.version");

        final var deprecations = assertOKAndCreateObjectPath(adminClient().performRequest(new Request("GET", "/_migration/deprecations")));
        final List<Object> nodeSettingsIssues = deprecations.evaluate("node_settings");
        if (Runtime.Version.parse(javaVersion).feature() >= 25) {
            assertThat(nodeSettingsIssues, empty());
        } else {
            assertThat(nodeSettingsIssues, hasSize(1));
            assertThat(deprecations.evaluate("node_settings.0.level"), equalTo("critical"));
            assertThat(deprecations.evaluate("node_settings.0.message"), equalTo("Running Elasticsearch with Java 21-24 is deprecated"));
            // nodes info reports java.version while the issue reports the full runtime version, which may have build info appended
            final String details = deprecations.evaluate("node_settings.0.details");
            assertThat(details, startsWith("This node is running Java [" + javaVersion));
            assertThat(details, containsString("]. A future Elasticsearch 9.x release will require Java 25 or later."));
            assertThat(deprecations.evaluate("node_settings.0.resolve_during_rolling_upgrade"), equalTo(false));
        }
    }
}
