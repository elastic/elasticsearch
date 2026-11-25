/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.ccq;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.AssertWarnings;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;

/**
 * Proves that {@code FILLNULL} runs across the BWC boundary even when the remote cluster predates the command.
 * <p>
 * {@code FILLNULL} is resolved entirely on the coordinating cluster: the coordinator parses and plans it, lowering it to
 * the {@code EVAL}/{@code COALESCE}-level primitives that every supported version already understands, so a remote never
 * receives a {@code FILLNULL} command of its own. A query that fills nulls in data drawn from a remote must therefore
 * succeed whatever version the remote runs, as long as the coordinator knows the command.
 * <p>
 * The suite runs in both BWC directions (see {@code build.gradle}). The interesting direction is {@code newToOld}
 * (coordinator current, remote on the BWC version), where the remote may not know {@code FILLNULL} at all. The
 * {@code oldToNew} direction is skipped on any old coordinator that cannot parse {@code FILLNULL}, since such a
 * coordinator could never run the query; the {@link #assumeCoordinatorSupportsFillNull()} capability check enforces that.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FillNullBwcIT extends ESRestTestCase {

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private static final String LOCAL_INDEX = "fillnull-bwc-local";
    private static final String REMOTE_INDEX = "fillnull-bwc-remote";

    // A keyword that every document carries and a long that some documents omit, so the query always has a null to fill.
    private static final String MAPPING = """
         "properties": {
           "name": { "type": "keyword" },
           "value": { "type": "long" }
         }
        """;

    @Override
    protected String getTestRestCluster() {
        return localCluster.getHttpAddresses();
    }

    @Before
    public void assumeCoordinatorSupportsFillNull() throws IOException {
        assumeTrue(
            "the coordinating cluster must support FILLNULL to run the command",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.FILLNULL.capabilityName()))
        );
    }

    @Before
    public void setUpIndices() throws IOException {
        createIndex(client(), LOCAL_INDEX, Settings.EMPTY, MAPPING, null);
        indexDoc(client(), LOCAL_INDEX, "4", "{\"name\":\"local-a\",\"value\":40}");
        indexDoc(client(), LOCAL_INDEX, "5", "{\"name\":\"local-b\"}"); // value omitted -> null
        refresh(client(), LOCAL_INDEX);

        try (RestClient remoteClient = remoteClusterClient()) {
            createIndex(remoteClient, REMOTE_INDEX, Settings.EMPTY, MAPPING, null);
            indexDoc(remoteClient, REMOTE_INDEX, "1", "{\"name\":\"remote-a\",\"value\":10}");
            indexDoc(remoteClient, REMOTE_INDEX, "2", "{\"name\":\"remote-b\"}"); // value omitted -> null
            indexDoc(remoteClient, REMOTE_INDEX, "3", "{\"name\":\"remote-c\",\"value\":30}");
            refresh(remoteClient, REMOTE_INDEX);
        }
    }

    @After
    public void wipeIndices() throws IOException {
        deleteIndex(client(), LOCAL_INDEX);
        try (RestClient remoteClient = remoteClusterClient()) {
            deleteIndex(remoteClient, REMOTE_INDEX);
        }
    }

    /**
     * The null on the (possibly old) remote is filled by the coordinator.
     */
    public void testFillNullOverRemoteIndex() throws IOException {
        Map<String, Object> result = runEsql(Strings.format("""
            FROM *:%s
            | FILLNULL 0 ON value
            | SORT name
            | KEEP name, value
            """, REMOTE_INDEX));
        assertEquals(List.of(Map.of("name", "name", "type", "keyword"), Map.of("name", "value", "type", "long")), result.get("columns"));
        assertEquals(List.of(List.of("remote-a", 10), List.of("remote-b", 0), List.of("remote-c", 30)), result.get("values"));
    }

    /**
     * Nulls on both the local and the remote side of a union are filled together.
     */
    public void testFillNullOverLocalAndRemoteIndices() throws IOException {
        Map<String, Object> result = runEsql(Strings.format("""
            FROM %s,*:%s
            | FILLNULL 0 ON value
            | SORT name
            | KEEP name, value
            """, LOCAL_INDEX, REMOTE_INDEX));
        assertEquals(List.of(Map.of("name", "name", "type", "keyword"), Map.of("name", "value", "type", "long")), result.get("columns"));
        assertEquals(
            List.of(
                List.of("local-a", 40),
                List.of("local-b", 0),
                List.of("remote-a", 10),
                List.of("remote-b", 0),
                List.of("remote-c", 30)
            ),
            result.get("values")
        );
    }

    /**
     * The filled value is materialized, not merely displayed: a downstream filter on it matches the formerly-null row.
     */
    public void testFilledRemoteValueIsQueryable() throws IOException {
        Map<String, Object> result = runEsql(Strings.format("""
            FROM *:%s
            | FILLNULL 0 ON value
            | WHERE value == 0
            | STATS c = COUNT(*)
            """, REMOTE_INDEX));
        assertEquals(List.of(Map.of("name", "c", "type", "long")), result.get("columns"));
        assertEquals(List.of(List.of(1)), result.get("values"));
    }

    private Map<String, Object> runEsql(String query) throws IOException {
        return RestEsqlTestCase.runEsqlSync(
            new RestEsqlTestCase.RequestObjectBuilder().query(query),
            new AssertWarnings.NoWarnings(),
            null
        );
    }

    private static void indexDoc(RestClient client, String index, String id, String json) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + id);
        request.setJsonEntity(json);
        assertOK(client.performRequest(request));
    }

    private RestClient remoteClusterClient() throws IOException {
        var clusterHosts = parseClusterHosts(remoteCluster.getHttpAddresses());
        return buildClient(restClientSettings(), clusterHosts.toArray(new HttpHost[0]));
    }
}
