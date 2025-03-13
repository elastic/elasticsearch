/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;

/**
 * This test ensure that we keep the search states of a CCS request correctly when the local and remote clusters
 * have different but compatible versions. See SearchService#createAndPutReaderContext
 */
public class AggregationsIT extends ESRestTestCase {

    private static final String CLUSTER_ALIAS = "remote_cluster";
    private static final String localIndex = "test_bwc_index";
    private static final String remoteIndex = "test_bwc_remote_index";
    private static final String queryIndices = URLEncoder.encode(localIndex + ",remote_cluster:" + remoteIndex, StandardCharsets.UTF_8);
    private static int docs;

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    static List<SearchStatesIT.Node> getNodes(RestClient restClient) throws IOException {
        Response response = restClient.performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        final Map<String, Object> nodeMap = objectPath.evaluate("nodes");
        final List<SearchStatesIT.Node> nodes = new ArrayList<>();
        for (String id : nodeMap.keySet()) {
            final String name = objectPath.evaluate("nodes." + id + ".name");
            final Version version = Version.fromString(objectPath.evaluate("nodes." + id + ".version"));
            final String transportAddress = objectPath.evaluate("nodes." + id + ".transport.publish_address");
            final String httpAddress = objectPath.evaluate("nodes." + id + ".http.publish_address");
            final Map<String, Object> attributes = objectPath.evaluate("nodes." + id + ".attributes");
            nodes.add(new SearchStatesIT.Node(id, name, version, transportAddress, httpAddress, attributes));
        }
        return nodes;
    }

    static List<HttpHost> parseHosts(String props) {
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

    static RestClient newLocalClient() {
        return RestClient.builder(randomFrom(parseHosts("tests.rest.cluster"))).build();
    }

    static RestClient newRemoteClient() {
        return RestClient.builder(randomFrom(parseHosts("tests.rest.remote_cluster"))).build();
    }

    @Before
    private void configureClusters() throws Exception {
        if (docs == 0) {
            try (RestClient localClient = newLocalClient(); RestClient remoteClient = newRemoteClient()) {
                configureRemoteClusters(localClient, getNodes(remoteClient));
                docs = between(10, 100);
                createindex(localClient, localIndex);
                createindex(remoteClient, remoteIndex);
            }
        }
    }

    @After
    private void clearClusters() throws Exception {
        try (RestClient localClient = newLocalClient(); RestClient remoteClient = newRemoteClient()) {
            deleteIndex(localClient, localIndex);
            deleteIndex(remoteClient, remoteIndex);
            docs = 0;
        }
    }

    private void createindex(RestClient client, String index) throws IOException {
        final String mapping = """
             "properties": {
               "date": { "type": "date" },
               "number": { "type": "integer" },
               "keyword": { "type": "keyword" }
             }
            """;
        createIndex(client, index, Settings.EMPTY, mapping);
        for (int i = 0; i < docs; i++) {
            Request createDoc = new Request("POST", "/" + index + "/_doc/id_" + i);
            createDoc.setJsonEntity(Strings.format("""
                { "date": %s, "number": %s, "keyword" : %s }
                """, i * 1000 * 60, i, "" + i));
            assertOK(client.performRequest(createDoc));
        }
        refresh(client, index);
    }

    private static void configureRemoteClusters(RestClient localClient, List<SearchStatesIT.Node> remoteNodes) throws Exception {
        final String remoteClusterSettingPrefix = "cluster.remote." + CLUSTER_ALIAS + ".";
        final Settings remoteConnectionSettings;
        final List<String> seeds = remoteNodes.stream()
            .filter(n -> n.attributes().containsKey("gateway"))
            .map(n -> n.transportAddress())
            .collect(Collectors.toList());
        remoteConnectionSettings = Settings.builder()
            .putNull(remoteClusterSettingPrefix + "proxy_address")
            .put(remoteClusterSettingPrefix + "mode", "sniff")
            .putList(remoteClusterSettingPrefix + "seeds", seeds)
            .build();
        updateClusterSettings(localClient, remoteConnectionSettings);
        assertBusy(() -> {
            final Response resp = localClient.performRequest(new Request("GET", "/_remote/info"));
            assertOK(resp);
            final ObjectPath objectPath = ObjectPath.createFromResponse(resp);
            assertNotNull(objectPath.evaluate(CLUSTER_ALIAS));
            assertTrue(objectPath.evaluate(CLUSTER_ALIAS + ".connected"));
        }, 60, TimeUnit.SECONDS);
    }

    public void testDateHistogram() throws Exception {
        for (int i = 0; i < 3; i++) {
            try (RestClient localClient = newLocalClient()) {
                Request request = new Request("POST", "/" + queryIndices + "/_search");
                request.setJsonEntity("""
                        {
                          "aggs": {
                            "hist": {
                              "date_histogram": {
                                "field": "date",
                                "calendar_interval":  "minute"
                              }
                            }
                          }
                        }
                    """);
                ObjectPath response = ObjectPath.createFromResponse(localClient.performRequest(request));
                assertEquals(docs, response.evaluateArraySize("aggregations.hist.buckets"));
                for (int j = 0; j < docs; j++) {
                    assertEquals(2, (int) response.evaluate("aggregations.hist.buckets." + j + ".doc_count"));
                }
            }
        }
    }

    public void testHistogram() throws Exception {
        for (int i = 0; i < 3; i++) {
            try (RestClient localClient = newLocalClient()) {
                Request request = new Request("POST", "/" + queryIndices + "/_search");
                request.setJsonEntity("""
                        {
                          "aggs": {
                            "hist": {
                              "histogram": {
                                "field": "number",
                                "interval": 1
                              }
                            }
                          }
                        }
                    """);
                ObjectPath response = ObjectPath.createFromResponse(localClient.performRequest(request));
                assertEquals(docs, response.evaluateArraySize("aggregations.hist.buckets"));
                for (int j = 0; j < docs; j++) {
                    assertEquals(2, (int) response.evaluate("aggregations.hist.buckets." + j + ".doc_count"));
                }
            }
        }
    }

    public void testTerms() throws Exception {
        for (int i = 0; i < 3; i++) {
            try (RestClient localClient = newLocalClient()) {
                Request request = new Request("POST", "/" + queryIndices + "/_search");
                request.setJsonEntity("""
                        {
                          "aggs": {
                            "terms": {
                              "terms": {
                                "field": "keyword",
                                "size": 1000
                              }
                            }
                          }
                        }
                    """);
                ObjectPath response = ObjectPath.createFromResponse(localClient.performRequest(request));
                assertEquals(docs, response.evaluateArraySize("aggregations.terms.buckets"));
                for (int j = 0; j < docs; j++) {
                    assertEquals(2, (int) response.evaluate("aggregations.terms.buckets." + j + ".doc_count"));
                }
            }
        }
    }
}
