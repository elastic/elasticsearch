/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.rest;

import org.apache.http.HttpHost;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

public abstract class AbstractCCSRestTestCase extends ESRestTestCase {

    protected static class Node {
        public final String id;
        public final String name;
        public final Version version;
        public final String transportAddress;
        public final String httpAddress;
        public final Map<String, Object> attributes;

        Node(String id, String name, Version version, String transportAddress, String httpAddress, Map<String, Object> attributes) {
            this.id = id;
            this.name = name;
            this.version = version;
            this.transportAddress = transportAddress;
            this.httpAddress = httpAddress;
            this.attributes = attributes;
        }

        @Override
        public String toString() {
            return "Node{"
                + "id='"
                + id
                + '\''
                + ", name='"
                + name
                + '\''
                + ", version="
                + version
                + ", transportAddress='"
                + transportAddress
                + '\''
                + ", httpAddress='"
                + httpAddress
                + '\''
                + ", attributes="
                + attributes
                + '}';
        }
    }

    protected static List<Node> getNodes(RestClient restClient) throws IOException {
        Response response = restClient.performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> nodeMap = objectPath.evaluate("nodes");
        final List<Node> nodes = new ArrayList<>();
        for (String id : nodeMap.keySet()) {
            final String name = objectPath.evaluate("nodes." + id + ".name");
            final Version version = Version.fromString(objectPath.evaluate("nodes." + id + ".version"));
            final String transportAddress = objectPath.evaluate("nodes." + id + ".transport.publish_address");
            final String httpAddress = objectPath.evaluate("nodes." + id + ".http.publish_address");
            final Map<String, Object> attributes = objectPath.evaluate("nodes." + id + ".attributes");
            nodes.add(new Node(id, name, version, transportAddress, httpAddress, attributes));
        }
        return nodes;
    }

    protected static List<HttpHost> parseHosts(String props) {
        final String address = System.getProperty(props);
        assertNotNull("[" + props + "] is not configured", address);
        String[] stringUrls = address.split(",");
        List<HttpHost> hosts = new ArrayList<>(stringUrls.length);
        for (String stringUrl : stringUrls) {
            int portSeparator = stringUrl.lastIndexOf(':');
            if (portSeparator < 0) {
                throw new IllegalArgumentException("Illegal cluster url [" + stringUrl + "]");
            }
            String host = stringUrl.substring(0, portSeparator);
            int port = Integer.parseInt(stringUrl.substring(portSeparator + 1));
            hosts.add(new HttpHost(host, port, "http"));
        }
        assertThat("[" + props + "] is empty", hosts, not(empty()));
        return hosts;
    }

    private static RestClient createLocalClient(Logger logger) {
        final List<HttpHost> hosts = parseHosts("tests.rest.cluster");
        final int index = random().nextInt(hosts.size());
        logger.info("Using client node {}", index);
        return RestClient.builder(hosts.get(index)).build();
    }

    public static void configureRemoteClusters(List<Node> remoteNodes, String remoteClusterAlias, Version upgradeFromVersion, Logger logger)
        throws Exception {
        final String remoteClusterSettingPrefix = "cluster.remote." + remoteClusterAlias + ".";
        try (RestClient localClient = createLocalClient(logger)) {
            final Settings remoteConnectionSettings;
            if (upgradeFromVersion.before(Version.V_7_6_0) || randomBoolean()) {
                final List<String> seeds = remoteNodes.stream()
                    .filter(n -> n.attributes.containsKey("gateway"))
                    .map(n -> n.transportAddress)
                    .collect(Collectors.toList());
                logger.info("--> use sniff mode with seed [{}], remote nodes [{}]", seeds, remoteNodes);
                if (upgradeFromVersion.before(Version.V_7_6_0)) {
                    remoteConnectionSettings = Settings.builder().putList(remoteClusterSettingPrefix + "seeds", seeds).build();
                } else {
                    remoteConnectionSettings = Settings.builder()
                        .putNull(remoteClusterSettingPrefix + "proxy_address")
                        .put(remoteClusterSettingPrefix + "mode", "sniff")
                        .putList(remoteClusterSettingPrefix + "seeds", seeds)
                        .build();
                }
            } else {
                final Node proxyNode = randomFrom(remoteNodes);
                logger.info("--> use proxy node [{}], remote nodes [{}]", proxyNode, remoteNodes);
                remoteConnectionSettings = Settings.builder()
                    .putNull(remoteClusterSettingPrefix + "seeds")
                    .put(remoteClusterSettingPrefix + "mode", "proxy")
                    .put(remoteClusterSettingPrefix + "proxy_address", proxyNode.transportAddress)
                    .build();
            }
            ClusterUpdateSettingsRequest settingsRequest = new ClusterUpdateSettingsRequest().persistentSettings(remoteConnectionSettings);
            assertBusy(() -> {
                Request request = new Request("PUT", "/_cluster/settings");
                request.setJsonEntity(Strings.toString(settingsRequest));
                final Response resp = localClient.performRequest(request);
                assertOK(resp);
            }, 60, TimeUnit.SECONDS);
            assertBusy(() -> {
                final Response resp = localClient.performRequest(new Request("GET", "/_remote/info"));
                assertOK(resp);
                final ObjectPath objectPath = ObjectPath.createFromResponse(resp);
                assertNotNull(objectPath.evaluate(remoteClusterAlias));
                assertTrue(objectPath.evaluate(remoteClusterAlias + ".connected"));
            }, 60, TimeUnit.SECONDS);
        }
    }
}
