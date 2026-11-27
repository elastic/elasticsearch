/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * An explicit cast is lenient on the type of its input only when every node of the cluster can do it: the coordinator decides it
 * from the minimum version of the nodes, and sends the plan to all of them. A cluster with a node that can't keeps failing the casts
 * the way it always did, whichever node coordinates the query. Once every node can, the same query returns {@code null}.
 * <p>
 * Each node is asked whether it has the capability itself, so the test also runs in a cluster whose old nodes report the current
 * version, like a cluster upgraded from a detached build of main.
 * <p>
 * The serverless upgrade tests run this class against a stateless cluster, with their own task that a Gradle filter here doesn't
 * reach, so it skips itself there. In a stateless cluster an index without replicas has no search shard.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class LenientCastMixedClusterIT extends ESRestTestCase {
    private static final String INDEX = "lenient_cast_mixed";
    /** {@code value} is an {@code ip} here, and a {@code keyword} in {@link #KEYWORD_INDEX}. */
    private static final String IP_INDEX = "lenient_cast_mixed_ip";
    private static final String KEYWORD_INDEX = "lenient_cast_mixed_keyword";
    private static final String LENIENT_CAST = EsqlCapabilities.Cap.LENIENT_CAST.capabilityName();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Before
    public void createIndicesOnce() throws IOException {
        // first, so a stateless cluster doesn't get an index it can't search
        assumeFalse("the cluster is stateless", isStateless());
        createIndexOnce(INDEX, "{\"id\": {\"type\": \"integer\"}}", "{\"id\": 1}");
        createIndexOnce(IP_INDEX, "{\"value\": {\"type\": \"ip\"}}", "{\"value\": \"10.0.0.1\"}");
        createIndexOnce(KEYWORD_INDEX, "{\"value\": {\"type\": \"keyword\"}}", "{\"value\": \"7\"}");
    }

    /**
     * {@code id} is an {@code integer}, which {@code TO_IP} doesn't convert.
     */
    public void testCastOfASingleType() throws IOException {
        String error = "found value [id] type [integer]";
        assertLenientOnlyWhenEveryNodeCan(
            "FROM " + INDEX + " | EVAL x = id::ip | KEEP x | LIMIT 10",
            List.of(Collections.singletonList(null)),
            error
        );
        assertLenientOnlyWhenEveryNodeCan(
            "FROM " + INDEX + " | EVAL x = TO_IP(id) | KEEP x | LIMIT 10",
            List.of(Collections.singletonList(null)),
            error
        );
    }

    /**
     * A field mapped to several types is {@code null} in the indices that map a type the cast doesn't convert, which is an {@code ip}
     * for {@code TO_LONG}.
     */
    public void testCastOfAFieldMappedToSeveralTypes() throws IOException {
        assertLenientOnlyWhenEveryNodeCan(
            "FROM " + IP_INDEX + "," + KEYWORD_INDEX + " METADATA _index | EVAL x = value::long | SORT _index | KEEP _index, x",
            List.of(Arrays.asList(IP_INDEX, null), Arrays.asList(KEYWORD_INDEX, 7))
        );
    }

    /**
     * Runs {@code query} with each node as the coordinator. It returns {@code expectedValues} when every node has the lenient casts,
     * and fails with a verification error that contains {@code expectedError} otherwise.
     */
    private void assertLenientOnlyWhenEveryNodeCan(String query, List<?> expectedValues, String expectedError) throws IOException {
        List<String> nodes = httpAddresses();
        boolean everyNodeCan = true;
        for (String node : nodes) {
            everyNodeCan &= hasLenientCast(node);
        }
        for (String node : nodes) {
            try (RestClient coordinator = clientOf(node)) {
                Request request = new Request("POST", "/_query");
                request.setJsonEntity("{\"query\": \"" + query + "\"}");
                // The lenient cast warns
                request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE));
                String message = "query [" + query + "] coordinated by [" + node + "], every node can [" + everyNodeCan + "]";
                if (everyNodeCan) {
                    Map<String, Object> response = entityAsMap(coordinator.performRequest(request));
                    assertThat(message, response.get("values"), equalTo(expectedValues));
                } else {
                    ResponseException e = expectThrows(ResponseException.class, message, () -> coordinator.performRequest(request));
                    assertThat(message, e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
                    assertThat(message, e.getMessage(), containsString("verification_exception"));
                    assertThat(message, e.getMessage(), containsString(expectedError));
                }
            }
        }
    }

    private void createIndexOnce(String name, String properties, String document) throws IOException {
        if (indexExists(name)) {
            return;
        }
        // The shard is on a node that has the lenient casts, while a node that doesn't is in the cluster, so the test fails if the
        // coordinator only looks at the nodes that serve the query, and not at the whole cluster.
        String node = nodeWithLenientCast();
        String allocation = node == null ? "" : ", \"index.routing.allocation.include._name\": \"" + node + "\"";
        Request create = new Request("PUT", "/" + name);
        create.setJsonEntity("""
            {
              "settings": {"number_of_shards": 1, "number_of_replicas": 0%s},
              "mappings": {"properties": %s}
            }""".formatted(allocation, properties));
        client().performRequest(create);
        Request index = new Request("POST", "/" + name + "/_doc");
        index.addParameter("refresh", "true");
        index.setJsonEntity(document);
        client().performRequest(index);
    }

    /**
     * The name of a node that has the lenient casts itself, or {@code null} if no node has them.
     */
    private String nodeWithLenientCast() throws IOException {
        ObjectPath info = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> all = info.evaluate("nodes");
        for (String id : all.keySet()) {
            if (hasLenientCast(info.evaluate("nodes." + id + ".http.publish_address"))) {
                return info.evaluate("nodes." + id + ".name");
            }
        }
        return null;
    }

    /**
     * The HTTP addresses of the nodes of the cluster.
     */
    private List<String> httpAddresses() throws IOException {
        ObjectPath info = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> all = info.evaluate("nodes");
        List<String> addresses = new ArrayList<>();
        for (String id : all.keySet()) {
            addresses.add(info.evaluate("nodes." + id + ".http.publish_address"));
        }
        return addresses;
    }

    /**
     * Whether the node at {@code httpAddress} has the lenient casts itself, whatever the other nodes have. A node too old to know
     * {@code local_only} answers 4xx, and has no lenient casts either.
     */
    private boolean hasLenientCast(String httpAddress) throws IOException {
        Request request = new Request("GET", "_capabilities");
        request.addParameter("method", "POST");
        request.addParameter("path", "/_query");
        request.addParameter("capabilities", LENIENT_CAST);
        request.addParameter("local_only", "true");
        try (RestClient node = clientOf(httpAddress)) {
            return Boolean.TRUE.equals(entityAsMap(node.performRequest(request)).get("supported"));
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() / 100 == 4) {
                return false;
            }
            throw e;
        }
    }

    private RestClient clientOf(String httpAddress) throws IOException {
        return buildClient(restClientSettings(), parseClusterHosts(httpAddress).toArray(HttpHost[]::new));
    }

    /**
     * Whether the cluster is stateless: some node has the role of indexing or searching shards.
     */
    private static boolean isStateless() throws IOException {
        ObjectPath info = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> nodes = info.evaluate("nodes");
        for (String id : nodes.keySet()) {
            List<String> roles = info.evaluate("nodes." + id + ".roles");
            if (roles.contains(DiscoveryNodeRole.INDEX_ROLE.roleName()) || roles.contains(DiscoveryNodeRole.SEARCH_ROLE.roleName())) {
                return true;
            }
        }
        return false;
    }
}
