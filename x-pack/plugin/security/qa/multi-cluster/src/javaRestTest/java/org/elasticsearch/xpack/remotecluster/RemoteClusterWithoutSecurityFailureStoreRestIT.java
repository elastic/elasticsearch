/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RemoteClusterWithoutSecurityFailureStoreRestIT extends ESRestTestCase {

    private static ElasticsearchCluster fulfillingCluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("fulfilling-cluster")
        .nodes(3)
        .module("analysis-common")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .build();

    private static ElasticsearchCluster queryCluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .name("query-cluster")
        .module("analysis-common")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "false")
        .build();

    private static RestClient fulfillingClusterClient;

    @BeforeClass
    public static void initFulfillingClusterClient() {
        if (fulfillingClusterClient != null) {
            return;
        }
        fulfillingClusterClient = buildRestClient(fulfillingCluster);
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    static RestClient buildRestClient(ElasticsearchCluster targetCluster) {
        assert targetCluster != null;
        final int numberOfFcNodes = targetCluster.getHttpAddresses().split(",").length;
        final String url = targetCluster.getHttpAddress(randomIntBetween(0, numberOfFcNodes - 1));

        final int portSeparator = url.lastIndexOf(':');
        final var httpHost = new HttpHost(url.substring(0, portSeparator), Integer.parseInt(url.substring(portSeparator + 1)), "http");
        RestClientBuilder builder = RestClient.builder(httpHost);
        try {
            doConfigureClient(builder, Settings.EMPTY);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        builder.setStrictDeprecationMode(true);
        return builder.build();
    }

    @AfterClass
    public static void closeFulfillingClusterClient() throws IOException {
        try {
            IOUtils.close(fulfillingClusterClient);
        } finally {
            fulfillingClusterClient = null;
        }
    }

    @Override
    protected String getTestRestCluster() {
        return queryCluster.getHttpAddress(0);
    }

    public void testCrossClusterSearchWithoutSecurity() throws Exception {
        final boolean isProxyMode = randomBoolean();
        final boolean skipUnavailable = false; // we want to get actual failures and not skip and get empty results
        final boolean ccsMinimizeRoundtrips = randomBoolean();

        configureRemoteCluster("my_remote_cluster", fulfillingCluster, isProxyMode, skipUnavailable);

        // fulfilling cluster setup
        setupTestDataStreamOnFulfillingCluster();

        // query cluster setup
        setupLocalDataOnQueryCluster();

        // query remote cluster using :: selectors should not succeed, even with security off
        for (String indexName : Set.of(
            "test1::data",
            "test*::data",
            "*::data",
            "test1::failures",
            "test*::failures",
            "*::failures",
            "other1::failures",
            "non-existing::whatever"
        )) {
            final Request searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    indexName,
                    ccsMinimizeRoundtrips
                )
            );
            final ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(searchRequest));
            assertSelectorsNotSupported(exception);
        }

        final Tuple<String, String> backingIndices = getSingleDataAndFailureIndices("test1");
        final String backingDataIndexName = backingIndices.v1();
        final String backingFailureIndexName = backingIndices.v2();

        // searching without selectors should work
        {
            final boolean alsoSearchLocally = randomBoolean();
            final Request dataSearchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s%s:%s/_search?ccs_minimize_roundtrips=%s",
                    alsoSearchLocally ? "local_index," : "",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    randomFrom("test1", "test*", "*", backingDataIndexName),
                    ccsMinimizeRoundtrips
                )
            );
            final String[] expectedIndices = alsoSearchLocally
                ? new String[] { "local_index", backingDataIndexName }
                : new String[] { backingDataIndexName };
            assertSearchResponseContainsIndices(client().performRequest(dataSearchRequest), expectedIndices);
        }

        // also, searching directly the backing failure index should work
        {
            Request failureIndexSearchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/my_remote_cluster:%s/_search?ccs_minimize_roundtrips=%s",
                    backingFailureIndexName,
                    ccsMinimizeRoundtrips
                )
            );
            assertSearchResponseContainsIndices(client().performRequest(failureIndexSearchRequest), backingFailureIndexName);
        }
    }

    private static void setupLocalDataOnQueryCluster() throws IOException {
        final var indexDocRequest = new Request("POST", "/local_index/_doc?refresh=true");
        indexDocRequest.setJsonEntity("{\"local_foo\": \"local_bar\"}");
        assertOK(client().performRequest(indexDocRequest));
    }

    protected void setupTestDataStreamOnFulfillingCluster() throws IOException {
        // Create data stream and index some documents
        final Request createComponentTemplate = new Request("PUT", "/_component_template/component1");
        createComponentTemplate.setJsonEntity("""
            {
                "template": {
                    "mappings": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            },
                            "age": {
                                "type": "integer"
                            },
                            "email": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text"
                            }
                        }
                    },
                    "data_stream_options": {
                        "failure_store": {
                            "enabled": true
                        }
                    }
                }
            }""");
        assertOK(performRequestAgainstFulfillingCluster(createComponentTemplate));

        final Request createTemplate = new Request("PUT", "/_index_template/template1");
        createTemplate.setJsonEntity("""
            {
                "index_patterns": ["test*"],
                "data_stream": {},
                "priority": 500,
                "composed_of": ["component1"]
            }""");
        assertOK(performRequestAgainstFulfillingCluster(createTemplate));

        final Request createDoc1 = new Request("PUT", "/test1/_doc/1?refresh=true&op_type=create");
        createDoc1.setJsonEntity("""
            {
                "@timestamp": 1,
                "age" : 1,
                "name" : "jack",
                "email" : "jack@example.com"
            }""");
        assertOK(performRequestAgainstFulfillingCluster(createDoc1));

        final Request createDoc2 = new Request("PUT", "/test1/_doc/2?refresh=true&op_type=create");
        createDoc2.setJsonEntity("""
            {
                "@timestamp": 2,
                "age" : "this should be an int",
                "name" : "jack",
                "email" : "jack@example.com"
            }""");
        assertOK(performRequestAgainstFulfillingCluster(createDoc2));
    }

    private static void assertSelectorsNotSupported(ResponseException exception) {
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(exception.getMessage(), containsString("Selectors are not yet supported on remote cluster patterns"));
    }

    private static void assertSearchResponseContainsIndices(Response response, String... expectedIndices) throws IOException {
        assertOK(response);
        final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
        try {
            final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getIndex)
                .collect(Collectors.toList());
            assertThat(actualIndices, containsInAnyOrder(expectedIndices));
        } finally {
            searchResponse.decRef();
        }
    }

    private static Response performRequestAgainstFulfillingCluster(Request request) throws IOException {
        return fulfillingClusterClient.performRequest(request);
    }

    private void configureRemoteCluster(
        String clusterAlias,
        ElasticsearchCluster targetFulfillingCluster,
        boolean isProxyMode,
        boolean skipUnavailable
    ) throws Exception {
        // For configurable remote cluster security, this method assumes the cross cluster access API key is already configured in keystore
        putRemoteClusterSettings(clusterAlias, targetFulfillingCluster, isProxyMode, skipUnavailable);

        // Ensure remote cluster is connected
        checkRemoteConnection(clusterAlias, targetFulfillingCluster, isProxyMode);
    }

    private void putRemoteClusterSettings(
        String clusterAlias,
        ElasticsearchCluster targetFulfillingCluster,
        boolean isProxyMode,
        boolean skipUnavailable
    ) throws IOException {
        final Settings.Builder builder = Settings.builder();
        final String remoteClusterEndpoint = targetFulfillingCluster.getTransportEndpoint(0);
        if (isProxyMode) {
            builder.put("cluster.remote." + clusterAlias + ".mode", "proxy")
                .put("cluster.remote." + clusterAlias + ".proxy_address", remoteClusterEndpoint)
                .putNull("cluster.remote." + clusterAlias + ".seeds");
        } else {
            builder.put("cluster.remote." + clusterAlias + ".mode", "sniff")
                .putList("cluster.remote." + clusterAlias + ".seeds", remoteClusterEndpoint)
                .putNull("cluster.remote." + clusterAlias + ".proxy_address");
        }
        builder.put("cluster.remote." + clusterAlias + ".skip_unavailable", skipUnavailable);
        updateClusterSettings(builder.build());
    }

    private void checkRemoteConnection(String clusterAlias, ElasticsearchCluster targetFulfillingCluster, boolean isProxyMode)
        throws Exception {
        final Request remoteInfoRequest = new Request("GET", "/_remote/info");
        assertBusy(() -> {
            final Response remoteInfoResponse = adminClient().performRequest(remoteInfoRequest);
            assertOK(remoteInfoResponse);
            final ObjectPath remoteInfoObjectPath = assertOKAndCreateObjectPath(remoteInfoResponse);
            assertThat(remoteInfoObjectPath.evaluate(clusterAlias + ".connected"), is(true));
            if (false == isProxyMode) {
                int numberOfFcNodes = (int) Arrays.stream(targetFulfillingCluster.getRemoteClusterServerEndpoints().split(","))
                    .filter(endpoint -> endpoint.length() > 0)
                    .count();
                if (numberOfFcNodes == 0) {
                    // The cluster is an RCS 1.0 remote cluster
                    numberOfFcNodes = targetFulfillingCluster.getTransportEndpoints().split(",").length;
                }
                assertThat(remoteInfoObjectPath.evaluate(clusterAlias + ".num_nodes_connected"), equalTo(numberOfFcNodes));
            }
            final String credentialsValue = remoteInfoObjectPath.evaluate(clusterAlias + ".cluster_credentials");
            assertThat(credentialsValue, nullValue());
        });
    }

    @SuppressWarnings("unchecked")
    private Tuple<List<String>, List<String>> getDataAndFailureIndices(String dataStreamName) throws IOException {
        Request dataStream = new Request("GET", "/_data_stream/" + dataStreamName);
        Response response = performRequestAgainstFulfillingCluster(dataStream);
        Map<String, Object> dataStreams = entityAsMap(response);
        List<String> dataIndexNames = (List<String>) XContentMapValues.extractValue("data_streams.indices.index_name", dataStreams);
        List<String> failureIndexNames = (List<String>) XContentMapValues.extractValue(
            "data_streams.failure_store.indices.index_name",
            dataStreams
        );
        return new Tuple<>(dataIndexNames, failureIndexNames);
    }

    private Tuple<String, String> getSingleDataAndFailureIndices(String dataStreamName) throws IOException {
        Tuple<List<String>, List<String>> indices = getDataAndFailureIndices(dataStreamName);
        assertThat(indices.v1().size(), equalTo(1));
        assertThat(indices.v2().size(), equalTo(1));
        return new Tuple<>(indices.v1().get(0), indices.v2().get(0));
    }
}
