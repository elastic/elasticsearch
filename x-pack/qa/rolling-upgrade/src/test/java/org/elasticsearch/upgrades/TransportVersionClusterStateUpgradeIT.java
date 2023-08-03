/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.util.Maps;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;

public class TransportVersionClusterStateUpgradeIT extends AbstractUpgradeTestCase {

    public void testReadsInferredTransportVersions() throws IOException {
        assumeTrue("TransportVersion introduced in 8.8.0", UPGRADE_FROM_VERSION.before(Version.V_8_8_0));
        assumeTrue("This only has visible effects when upgrading beyond 8.8.0", TransportVersion.current().after(TransportVersion.V_8_8_0));
        assumeTrue("Only runs on the mixed cluster", CLUSTER_TYPE == ClusterType.MIXED);
        // if the master is not upgraded, and the secondary node is, then the cluster info from the secondary
        // should have inferred transport versions in it
        // rely on randomisation to hit this case at some point in testing

        Request request = new Request("GET", "/_cluster/state/nodes");
        Map<String, Object> masterResponse = entityAsMap(client().performRequest(request));
        assumeFalse("Master needs to not know about transport versions", masterResponse.containsKey("transport_versions"));

        request = new Request("GET", "/_cluster/state/nodes?local=true");
        Map<String, Object> localResponse = entityAsMap(client().performRequest(request));
        // should either be empty, or using inferred versions
        assumeTrue("Local node needs to know about transport versions", masterResponse.containsKey("transport_versions"));
        Map<?, Version> vs = Maps.transformValues(
            ((Map<?, ?>) localResponse.get("nodes")),
            v -> Version.fromString(((Map<?, ?>) v).get("version").toString())
        );
        Map<?, TransportVersion> tvs = ((List<?>) localResponse.get("transport_versions")).stream()
            .map(o -> (Map<?, ?>) o)
            .collect(Collectors.toMap(m -> m.get("node_id"), m -> TransportVersion.fromString(m.get("transport_version").toString())));

        for (var ver : vs.entrySet()) {
            if (ver.getValue().after(Version.V_8_8_0)) {
                assertThat(
                    "Node " + ver.getKey() + " should have an inferred transport version",
                    tvs.get(ver.getKey()),
                    equalTo(TransportVersion.V_8_8_0)
                );
            }
        }
    }

    public void testCompletesRealTransportVersions() throws IOException {
        assumeTrue("TransportVersion introduced in 8.8.0", UPGRADE_FROM_VERSION.before(Version.V_8_8_0));
        assumeTrue("This only has visible effects when upgrading beyond 8.8.0", TransportVersion.current().after(TransportVersion.V_8_8_0));
        assumeTrue("Only runs on the upgraded cluster", CLUSTER_TYPE == ClusterType.UPGRADED);
        // once everything is upgraded, the master should fill in the real transport versions

        Request request = new Request("GET", "/_cluster/state/nodes");
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        Map<?, TransportVersion> tvs = ((List<?>) response.get("transport_versions")).stream()
            .map(o -> (Map<?, ?>) o)
            .collect(Collectors.toMap(m -> m.get("node_id"), m -> TransportVersion.fromString(m.get("transport_version").toString())));

        assertThat(tvs + " should be updated", tvs.values(), everyItem(equalTo(TransportVersion.current())));
    }
}
