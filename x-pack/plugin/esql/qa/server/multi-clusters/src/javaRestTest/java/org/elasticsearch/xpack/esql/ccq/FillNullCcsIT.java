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
import org.elasticsearch.client.ResponseException;
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
import java.util.regex.Pattern;

import static org.elasticsearch.xpack.esql.ccq.Clusters.REMOTE_CLUSTER_NAME;
import static org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase.hasCapabilities;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Cross-cluster semantics of {@code FILLNULL}: the cases that only arise when the data spans clusters. A field mapped on
 * only one cluster, a field whose type differs between clusters, {@code ON *} with a cross-cluster index pattern and the
 * {@code _index} metadata column, and a skipped (unavailable) remote. These run current-to-current on every build and in
 * both BWC directions under the matrix (see {@code build.gradle}); {@link #assumeCoordinatorSupportsFillNull()} skips any
 * old coordinator that cannot parse the command.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FillNullCcsIT extends ESRestTestCase {

    static ElasticsearchCluster remoteCluster = Clusters.remoteCluster();
    static ElasticsearchCluster localCluster = Clusters.localCluster(remoteCluster);

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(remoteCluster).around(localCluster);

    private static final String LOCAL_INDEX = "fillnull-ccs-local";
    private static final String REMOTE_INDEX = "fillnull-ccs-remote";

    // "mixed" is a keyword on the local cluster and a long on the remote: a cross-cluster type conflict. "local_only" and
    // "remote_only" are each mapped on just one side, to exercise a field that is missing on the other cluster.
    private static final String LOCAL_MAPPING = """
         "properties": {
           "name": { "type": "keyword" },
           "value": { "type": "long" },
           "local_only": { "type": "keyword" },
           "mixed": { "type": "keyword" }
         }
        """;
    private static final String REMOTE_MAPPING = """
         "properties": {
           "name": { "type": "keyword" },
           "value": { "type": "long" },
           "remote_only": { "type": "keyword" },
           "mixed": { "type": "long" }
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
        createIndex(client(), LOCAL_INDEX, Settings.EMPTY, LOCAL_MAPPING, null);
        indexDoc(client(), LOCAL_INDEX, "l1", "{\"name\":\"l1\",\"value\":40,\"local_only\":\"L\",\"mixed\":\"m\"}");
        indexDoc(client(), LOCAL_INDEX, "l2", "{\"name\":\"l2\"}"); // everything else null
        refresh(client(), LOCAL_INDEX);

        try (RestClient remoteClient = remoteClusterClient()) {
            createIndex(remoteClient, REMOTE_INDEX, Settings.EMPTY, REMOTE_MAPPING, null);
            indexDoc(remoteClient, REMOTE_INDEX, "r1", "{\"name\":\"r1\",\"value\":10,\"remote_only\":\"R\",\"mixed\":99}");
            indexDoc(remoteClient, REMOTE_INDEX, "r2", "{\"name\":\"r2\"}"); // everything else null
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
     * A field mapped on only one cluster is null on the other cluster's rows; FILLNULL fills those along with the
     * explicit nulls.
     */
    public void testFieldMissingOnOneCluster() throws IOException {
        Map<String, Object> result = runEsql(Strings.format("""
            FROM %s,*:%s
            | FILLNULL "n/a" ON local_only, remote_only
            | SORT name
            | KEEP name, local_only, remote_only
            """, LOCAL_INDEX, REMOTE_INDEX), new AssertWarnings.NoWarnings());
        assertEquals(
            List.of(
                Map.of("name", "name", "type", "keyword"),
                Map.of("name", "local_only", "type", "keyword"),
                Map.of("name", "remote_only", "type", "keyword")
            ),
            result.get("columns")
        );
        assertEquals(
            List.of(List.of("l1", "L", "n/a"), List.of("l2", "n/a", "n/a"), List.of("r1", "n/a", "R"), List.of("r2", "n/a", "n/a")),
            result.get("values")
        );
    }

    /**
     * A field whose type conflicts across clusters is ambiguous. Targeting it explicitly is a verification error, exactly
     * as it is for any other command that references an ambiguous field: FILLNULL does not resolve the conflict.
     */
    public void testMixedTypeFieldTargetedFailsWithAmbiguityError() {
        ResponseException e = expectThrows(ResponseException.class, () -> runEsql(Strings.format("""
            FROM %s,*:%s
            | FILLNULL "x" ON mixed
            | STATS c = COUNT(*)
            """, LOCAL_INDEX, REMOTE_INDEX), new AssertWarnings.NoWarnings()));
        assertThat(e.getMessage(), containsString("Cannot use field [mixed] due to ambiguities"));
    }

    /**
     * The lenient {@code ON *} form does not reference the ambiguous field by name, so the query succeeds: the conflicting
     * field is left unchanged and reported in a warning, while the other columns are filled.
     */
    public void testMixedTypeFieldWithStarIsLeftUnchanged() throws IOException {
        AssertWarnings warnings = new AssertWarnings.AllowedRegexes(
            List.of(Pattern.compile(".*\\[FILLNULL\\] the fill value could not be applied to the following fields.*"))
        );
        Map<String, Object> result = runEsql(Strings.format("""
            FROM %s,*:%s
            | FILLNULL "x" ON *
            | STATS c = COUNT(*)
            """, LOCAL_INDEX, REMOTE_INDEX), warnings);
        assertEquals(List.of(Map.of("name", "c", "type", "long")), result.get("columns"));
        assertEquals(List.of(List.of(4)), result.get("values"));
    }

    /**
     * {@code ON *} over a cross-cluster index pattern fills the ordinary columns but leaves the {@code _index} metadata
     * column with its real, cluster-qualified values.
     */
    public void testFillNullOnStarLeavesMetadataIndexAlone() throws IOException {
        Map<String, Object> result = runEsql(Strings.format("""
            FROM %s,*:%s METADATA _index
            | KEEP name, _index
            | FILLNULL "zzz" ON *
            | STATS c = COUNT(*) BY _index
            | SORT _index
            """, LOCAL_INDEX, REMOTE_INDEX), new AssertWarnings.NoWarnings());
        assertEquals(List.of(Map.of("name", "c", "type", "long"), Map.of("name", "_index", "type", "keyword")), result.get("columns"));
        assertEquals(List.of(List.of(2, LOCAL_INDEX), List.of(2, REMOTE_CLUSTER_NAME + ":" + REMOTE_INDEX)), result.get("values"));
    }

    /**
     * With a skip_unavailable remote, an unresolved remote index yields a partial result; FILLNULL still fills the local
     * rows that did come back.
     */
    public void testFillNullWithSkippedRemote() throws IOException {
        Map<String, Object> result = RestEsqlTestCase.runEsqlSync(new RestEsqlTestCase.RequestObjectBuilder().query(Strings.format("""
            FROM %s,%s:no-such-index
            | FILLNULL 0 ON value
            | SORT name
            | KEEP name, value
            """, LOCAL_INDEX, REMOTE_CLUSTER_NAME)).includeCCSMetadata(true), new AssertWarnings.NoWarnings(), null);
        assertThat(result.get("is_partial"), equalTo(true));
        assertEquals(List.of(List.of("l1", 40), List.of("l2", 0)), result.get("values"));
    }

    private Map<String, Object> runEsql(String query, AssertWarnings assertWarnings) throws IOException {
        return RestEsqlTestCase.runEsqlSync(new RestEsqlTestCase.RequestObjectBuilder().query(query), assertWarnings, null);
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
