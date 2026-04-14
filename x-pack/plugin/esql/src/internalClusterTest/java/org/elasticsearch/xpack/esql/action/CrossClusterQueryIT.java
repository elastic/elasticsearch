/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.test.FailingFieldPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.plugin.TransportEsqlQueryAction;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.EXPLAIN;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINE_STATS;
import static org.elasticsearch.xpack.esql.action.EsqlCapabilities.Cap.INLINE_STATS_SUPPORTS_REMOTE;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class CrossClusterQueryIT extends AbstractCrossClusterTestCase {
    protected static final String IDX_ALIAS = "alias1";
    protected static final String FILTERED_IDX_ALIAS = "alias-filtered-1";

    @After
    public void ensureExchangesAreReleased() throws Exception {
        for (Map.Entry<String, InternalTestCluster> entry : clusters().entrySet()) {
            String clusterAlias = entry.getKey();
            InternalTestCluster testCluster = entry.getValue();
            for (String node : testCluster.getNodeNames()) {
                TransportEsqlQueryAction esqlQueryAction = testCluster.getInstance(TransportEsqlQueryAction.class, node);
                ExchangeService exchangeService = esqlQueryAction.exchangeService();
                assertBusy(() -> {
                    if (exchangeService.lifecycleState() == Lifecycle.State.STARTED) {
                        assertTrue(
                            "Leftover exchanges " + exchangeService + " on node " + node + " in cluster " + clusterAlias,
                            exchangeService.isEmpty()
                        );
                    }
                }, 5, TimeUnit.SECONDS);
            }
        }
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean(), REMOTE_CLUSTER_2, randomBoolean());
    }

    protected void assertClusterInfoSuccess(EsqlExecutionInfo.Cluster clusterInfo, int numShards, long overallTookMillis) {
        assertThat(clusterInfo.getIndexExpression(), equalTo("logs-*"));
        assertThat(clusterInfo.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
        super.assertClusterInfoSuccess(clusterInfo, numShards);
    }

    public void testSuccessfulPathways() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();
        try (EsqlQueryResponse resp = runQuery("from logs-*,c*:logs-* | stats sum (v)", requestIncludeMeta)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(330L)));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterInfoSuccess(remoteCluster, remoteNumShards, overallTookMillis);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);

            // ensure that the _clusters metadata is present only if requested
            assertClusterMetadataInResponse(resp, responseExpectMeta);
        }

        try (EsqlQueryResponse resp = runQuery("from logs-*,c*:logs-* | stats count(*) by tag | sort tag | keep tag", requestIncludeMeta)) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(2));
            assertThat(values.get(0), equalTo(List.of("local")));
            assertThat(values.get(1), equalTo(List.of("remote")));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterInfoSuccess(remoteCluster, remoteNumShards, overallTookMillis);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);

            // ensure that the _clusters metadata is present only if requested
            assertClusterMetadataInResponse(resp, responseExpectMeta);
        }
    }

    public void testSearchesAgainstNonMatchingIndicesWithLocalOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");

        {
            String q = "FROM nomatch," + localIndex;
            IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> runQuery(q, false));
            assertThat(e.getDetailedMessage(), containsString("no such index [nomatch]"));

            // MP TODO: am I able to fix this from the field-caps call? Yes, if we detect concrete vs. wildcard expressions in user query
            // TODO bug - this does not throw; uncomment this test once this is fixed:
            // AwaitsFix https://github.com/elastic/elasticsearch/issues/114495
            // String limit0 = q + " | LIMIT 0";
            // VerificationException ve = expectThrows(VerificationException.class, () -> runQuery(limit0, false));
            // assertThat(ve.getDetailedMessage(), containsString("No matching indices for [nomatch]"));
        }

        {
            // no failure since concrete index matches, so wildcard matching is lenient
            String q = "FROM nomatch*," + localIndex;
            try (EsqlQueryResponse resp = runQuery(q, false)) {
                assertThat(resp.columns().size(), greaterThanOrEqualTo(1));
                assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                assertThat(resp.getExecutionInfo().isCrossClusterSearch(), is(false));
            }

            String limit0 = q + " | LIMIT 0";
            try (EsqlQueryResponse resp = runQuery(limit0, false)) {
                assertThat(resp.columns().size(), greaterThanOrEqualTo(1));
                assertThat(getValuesList(resp).size(), equalTo(0));
                assertThat(resp.getExecutionInfo().isCrossClusterSearch(), is(false));
            }
        }
        {
            String q = "FROM nomatch";
            String expectedError = "Unknown index [nomatch]";
            expectVerificationExceptionForQuery(q, expectedError, false);
        }
        {
            String q = "FROM nomatch*";
            try (var response = runQuery(q, false)) {
                assertThat(response.columns().size(), equalTo(1));
                assertThat(getValuesList(response).size(), equalTo(0));
                assertThat(response.getExecutionInfo().isCrossClusterSearch(), is(false));
            }
        }
    }

    public void testSearchesAgainstIndicesWithNoMappingsSkipUnavailableTrue() throws Exception {
        int numClusters = 2;
        setupClusters(numClusters);
        Map<String, String> clusterToEmptyIndexMap = createEmptyIndicesWithNoMappings(numClusters);
        setSkipUnavailable(REMOTE_CLUSTER_1, randomBoolean());

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        try {
            String emptyIndex = clusterToEmptyIndexMap.get(REMOTE_CLUSTER_1);
            String q = Strings.format("FROM cluster-a:%s", emptyIndex);
            // query without referring to fields should work
            {
                String limit1 = q + " | LIMIT 1";
                try (EsqlQueryResponse resp = runQuery(limit1, requestIncludeMeta)) {
                    assertThat(resp.columns().size(), equalTo(1));
                    assertThat(resp.columns().get(0).name(), equalTo("<no-fields>"));
                    assertThat(getValuesList(resp).size(), equalTo(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(new ExpectedCluster(REMOTE_CLUSTER_1, emptyIndex, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0))
                    );
                }

                String limit0 = q + " | LIMIT 0";
                try (EsqlQueryResponse resp = runQuery(limit0, requestIncludeMeta)) {
                    assertThat(resp.columns().size(), equalTo(1));
                    assertThat(resp.columns().get(0).name(), equalTo("<no-fields>"));
                    assertThat(getValuesList(resp).size(), equalTo(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(new ExpectedCluster(REMOTE_CLUSTER_1, emptyIndex, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0))
                    );
                }
            }

            // query that refers to missing fields should throw:
            // "type": "verification_exception",
            // "reason": "Found 1 problem\nline 2:7: Unknown column [foo]",
            {
                String keepQuery = q + " | KEEP foo | LIMIT 100";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(keepQuery, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown column [foo]"));
            }

        } finally {
            clearSkipUnavailable();
        }
    }

    public void testSearchesAgainstNonMatchingIndices() throws Exception {
        int numClusters = 3;
        Map<String, Object> testClusterInfo = setupClusters(numClusters);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote1NumShards = (Integer) testClusterInfo.get("remote1.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");
        String localIndex = (String) testClusterInfo.get("local.index");
        String remote1Index = (String) testClusterInfo.get("remote1.index");
        String remote2Index = (String) testClusterInfo.get("remote2.index");

        createIndexAliases(numClusters);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // missing concrete local index is an error
        {
            String q = "FROM nomatch,cluster-a:" + remote1Index;
            String expectedError = "Unknown index [nomatch]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }

        // missing concrete remote index is fatal
        {
            String q = "FROM logs*,cluster-a:nomatch";
            String expectedError = "Unknown index [cluster-a:nomatch]";
            setSkipUnavailable(REMOTE_CLUSTER_1, false);
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
            setSkipUnavailable(REMOTE_CLUSTER_1, true);
            try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertExpectedClustersForMissingIndicesTests(
                    executionInfo,
                    List.of(
                        new ExpectedCluster(LOCAL_CLUSTER, "logs*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, localNumShards),
                        new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0)
                    )
                );

            }
        }

        // No error since local non-matching index has wildcard and the remote cluster index expression matches
        {
            String remote1IndexName = randomFrom(remote1Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
            String q = Strings.format("FROM nomatch*,%s:%s", REMOTE_CLUSTER_1, remote1IndexName);
            try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertExpectedClustersForMissingIndicesTests(
                    executionInfo,
                    List.of(
                        // local cluster is never marked as SKIPPED even when no matcing indices - just marked as 0 shards searched
                        new ExpectedCluster(LOCAL_CLUSTER, "nomatch*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                        new ExpectedCluster(
                            REMOTE_CLUSTER_1,
                            remote1IndexName,
                            EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                            remote1NumShards
                        )
                    )
                );
            }

            String limit0 = q + " | LIMIT 0";
            try (EsqlQueryResponse resp = runQuery(limit0, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), equalTo(0));
                assertThat(resp.columns().size(), greaterThan(0));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertExpectedClustersForMissingIndicesTests(
                    executionInfo,
                    List.of(
                        // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                        new ExpectedCluster(LOCAL_CLUSTER, "nomatch*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                        // LIMIT 0 searches always have total shards = 0
                        new ExpectedCluster(REMOTE_CLUSTER_1, remote1IndexName, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0)
                    )
                );
            }
        }

        // No error since remote non-matching index has wildcard and the local cluster index expression matches
        {
            String indexLoc = randomFrom(localIndex, IDX_ALIAS, FILTERED_IDX_ALIAS);
            String q = Strings.format("FROM %s,cluster-a:nomatch*", indexLoc);

            try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertExpectedClustersForMissingIndicesTests(
                    executionInfo,
                    List.of(
                        // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                        new ExpectedCluster(LOCAL_CLUSTER, indexLoc, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, localNumShards),
                        new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0)
                    )
                );
            }

            String limit0 = q + " | LIMIT 0";
            try (EsqlQueryResponse resp = runQuery(limit0, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), equalTo(0));
                assertThat(resp.columns().size(), greaterThan(0));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertExpectedClustersForMissingIndicesTests(
                    executionInfo,
                    List.of(
                        // LIMIT 0 searches always have total shards = 0
                        new ExpectedCluster(LOCAL_CLUSTER, indexLoc, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                        new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0)
                    )
                );
            }
        }

        // an error is thrown and skipped by remote if there is a concrete index that does not match
        try (var r = runQuery("FROM cluster-a:nomatch", requestIncludeMeta)) {
            assertThat(r.isPartial(), equalTo(true));
            assertThat(r.getExecutionInfo().getCluster("cluster-a").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        }

        // no error is thrown if there are no matching indices at all
        try (var r = runQuery("FROM cluster-a:nomatch*", requestIncludeMeta)) {
            assertThat(r.isPartial(), equalTo(false));
            assertThat(r.getExecutionInfo().getCluster("cluster-a").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        }

        // an error is thrown and skipped if there is a concrete index that does not match
        try (var r = runQuery("FROM nomatch*,cluster-a:nomatch", requestIncludeMeta)) {
            assertThat(r.isPartial(), equalTo(true));
            assertThat(r.getExecutionInfo().getCluster("cluster-a").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        }

        // no error is thrown if there are no matching indices at all - local with wildcard, remote with wildcard
        try (var r = runQuery("FROM nomatch*,cluster-a:nomatch*", requestIncludeMeta)) {
            assertThat(r.isPartial(), equalTo(false));
            assertThat(r.getExecutionInfo().getCluster("cluster-a").getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        }
        {
            String q = "FROM nomatch,cluster-a:nomatch";
            String expectedError = "Unknown index [nomatch]";// only local errors are thrown
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }
        {
            String q = "FROM nomatch,cluster-a:nomatch*";
            String expectedError = "Unknown index [nomatch]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }

        // --- test against 3 clusters

        // missing concrete index (on remote) is error
        {
            String localIndexName = randomFrom(localIndex, IDX_ALIAS, FILTERED_IDX_ALIAS);
            String remote2IndexName = randomFrom(remote2Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
            String q = Strings.format("FROM %s*,cluster-a:nomatch,%s:%s*", localIndexName, REMOTE_CLUSTER_2, remote2IndexName);
            String expectedError = "Unknown index [cluster-a:nomatch]";
            setSkipUnavailable(REMOTE_CLUSTER_1, false);
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
            setSkipUnavailable(REMOTE_CLUSTER_1, true);
            try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertExpectedClustersForMissingIndicesTests(
                    executionInfo,
                    List.of(
                        new ExpectedCluster(
                            LOCAL_CLUSTER,
                            localIndexName + "*",
                            EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                            localNumShards
                        ),
                        new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0),
                        new ExpectedCluster(
                            REMOTE_CLUSTER_2,
                            remote2IndexName + "*",
                            EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                            remote2NumShards
                        )
                    )
                );

            }
        }
    }

    record ExpectedCluster(String clusterAlias, String indexExpression, EsqlExecutionInfo.Cluster.Status status, Integer totalShards) {}

    /**
     * Runs the provided query, expecting a VerificationError. It then runs the same query with a "| LIMIT 0"
     * extra processing step to ensure that ESQL coordinator-only operations throw the same VerificationError.
     */
    protected void expectVerificationExceptionForQuery(String query, String error, Boolean requestIncludeMeta) {
        VerificationException e = expectThrows(VerificationException.class, () -> runQuery(query, requestIncludeMeta));
        assertThat(e.getDetailedMessage(), containsString(error));

        String limit0 = query + " | LIMIT 0";
        e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
        assertThat(e.getDetailedMessage(), containsString(error));
    }

    public void assertExpectedClustersForMissingIndicesTests(EsqlExecutionInfo executionInfo, List<ExpectedCluster> expected) {
        long overallTookMillis = executionInfo.overallTook().millis();
        assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

        Set<String> expectedClusterAliases = expected.stream().map(c -> c.clusterAlias()).collect(Collectors.toSet());
        assertThat(executionInfo.clusterAliases(), equalTo(expectedClusterAliases));

        boolean hasSkipped = false;
        for (ExpectedCluster expectedCluster : expected) {
            EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster(expectedCluster.clusterAlias());
            String msg = cluster.getClusterAlias();
            assertThat(msg, cluster.getIndexExpression(), equalTo(expectedCluster.indexExpression()));
            assertThat(msg, cluster.getStatus(), equalTo(expectedCluster.status()));
            assertThat(msg, cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(msg, cluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(msg, cluster.getTotalShards(), equalTo(expectedCluster.totalShards()));
            if (cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.SUCCESSFUL) {
                assertThat(msg, cluster.getSuccessfulShards(), equalTo(expectedCluster.totalShards()));
                assertThat(msg, cluster.getSkippedShards(), equalTo(0));
            } else if (cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.SKIPPED) {
                assertThat(msg, cluster.getSuccessfulShards(), equalTo(0));
                assertThat(msg, cluster.getSkippedShards(), equalTo(expectedCluster.totalShards()));
                assertThat(msg, cluster.getFailures().size(), equalTo(1));
                assertThat(msg, cluster.getFailures().get(0).getCause(), instanceOf(VerificationException.class));
                assertThat(msg, cluster.getFailures().get(0).getCause().getMessage(), containsString("Unknown index"));
                assertThat(msg, cluster.getFailures().get(0).getCause().getMessage(), containsString(expectedCluster.indexExpression()));
                hasSkipped = true;
            }
            // currently failed shards is always zero - change this once we start allowing partial data for individual shard failures
            assertThat(msg, cluster.getFailedShards(), equalTo(0));
        }
        assertThat(executionInfo.isPartial(), equalTo(hasSkipped));
    }

    public void testSearchesWhereNonExistentClusterIsSpecifiedWithWildcards() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // a query which matches no remote cluster is not a cross cluster search
        try (EsqlQueryResponse resp = runQuery("from logs-*,x*:no_such_index* | stats sum (v)", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(45L)));

            assertNotNull(executionInfo);
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER)));
            assertThat(executionInfo.isCrossClusterSearch(), is(false));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            // since this not a CCS, only the overall took time in the EsqlExecutionInfo matters
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        }

        // skip_un must be true for the next test or it will fail on "cluster-a:no_such_index*" with a
        // VerificationException because there are no matching indices for that skip_un=false cluster.
        setSkipUnavailable(REMOTE_CLUSTER_1, true);

        // cluster-foo* matches nothing and so should not be present in the EsqlExecutionInfo
        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs-*,no_such_index*,cluster-a:no_such_index*,cluster-foo*:* | STATS sum (v)",
                requestIncludeMeta
            )
        ) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(45L)));

            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.isPartial(), equalTo(false));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("no_such_index*"));
            assertClusterInfoSuccess(remoteCluster, 0);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*,no_such_index*"));
            assertClusterInfoSuccess(localCluster, localNumShards);
        } finally {
            clearSkipUnavailable();
        }
    }

    /**
     * Searches with LIMIT 0 are used by Kibana to get a list of columns. After the initial planning
     * (which involves cross-cluster field-caps calls), it is a coordinator only operation at query time
     * which uses a different pathway compared to queries that require data node (and remote data node) operations
     * at query time.
     *
     * Note: the tests covering "nonmatching indices" also do LIMIT 0 tests.
     * This one is mostly focuses on took time values.
     */
    public void testCCSExecutionOnSearchesWithLimit0() throws Exception {
        setupTwoClusters();
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // Ensure non-cross cluster queries have overall took time
        try (EsqlQueryResponse resp = runQuery("FROM logs* | LIMIT 0", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(false));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        }

        // ensure cross-cluster searches have overall took time and correct per-cluster details in EsqlExecutionInfo
        try (EsqlQueryResponse resp = runQuery("FROM logs-*,cluster-a:* | LIMIT 0", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.isPartial(), equalTo(false));
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("*"));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertClusterInfoSuccess(remoteCluster, 0);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, 0, overallTookMillis);
        }
    }

    public void testMetadataIndex() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        try (
            EsqlQueryResponse resp = runQuery(
                Strings.format("FROM logs-*,%s:logs-* METADATA _index | stats sum(v) by _index | sort _index", REMOTE_CLUSTER_1),
                requestIncludeMeta
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), equalTo(List.of(285L, "cluster-a:logs-2")));
            assertThat(values.get(1), equalTo(List.of(45L, "logs-1")));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.isPartial(), equalTo(false));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterInfoSuccess(remoteCluster, remoteNumShards, overallTookMillis);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);
        }
    }

    public void testProfile() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        assumeTrue("pragmas only enabled on snapshot builds", Build.current().isSnapshot());
        // uses shard partitioning as segments can be merged during these queries
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.DATA_PARTITIONING.getKey(), DataPartitioning.SHARD).build());
        // Use single replicas for the target indices, to make sure we hit the same set of target nodes
        client(LOCAL_CLUSTER).admin()
            .indices()
            .prepareUpdateSettings("logs-1")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put("index.routing.rebalance.enable", "none"))
            .get();
        waitForNoInitializingShards(client(LOCAL_CLUSTER), TimeValue.timeValueSeconds(30), "logs-1");
        client(REMOTE_CLUSTER_1).admin()
            .indices()
            .prepareUpdateSettings("logs-2")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put("index.routing.rebalance.enable", "none"))
            .get();
        waitForNoInitializingShards(client(REMOTE_CLUSTER_1), TimeValue.timeValueSeconds(30), "logs-2");
        final int localOnlyProfiles;
        {
            try (EsqlQueryResponse resp = runQuery(syncEsqlQueryRequest("FROM logs* | stats sum(v)").pragmas(pragmas).profile(true))) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(45L)));
                assertNotNull(resp.profile());
                List<DriverProfile> drivers = resp.profile().drivers();
                assertThat(drivers.size(), greaterThanOrEqualTo(2)); // one coordinator and at least one data
                localOnlyProfiles = drivers.size();

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertNull(remoteCluster);
                assertThat(executionInfo.isCrossClusterSearch(), is(false));
                assertThat(executionInfo.includeCCSMetadata(), is(false));
                // since this not a CCS, only the overall took time in the EsqlExecutionInfo matters
                assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
            }
        }
        final int remoteOnlyProfiles;
        {
            try (EsqlQueryResponse resp = runQuery(syncEsqlQueryRequest("FROM c*:logs-* | stats sum(v)").pragmas(pragmas).profile(true))) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(285L)));
                assertNotNull(resp.profile());
                List<DriverProfile> drivers = resp.profile().drivers();
                assertThat(drivers.size(), greaterThanOrEqualTo(3)); // two coordinators and at least one data
                remoteOnlyProfiles = drivers.size();

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), is(false));
                assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSuccess(remoteCluster, remoteNumShards, executionInfo.overallTook().millis());

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertNull(localCluster);
            }
        }
        final int allProfiles;
        {
            EsqlQueryRequest request = syncEsqlQueryRequest("FROM logs-*,c*:logs-* | stats total = sum(v)").pragmas(pragmas).profile(true);
            try (EsqlQueryResponse resp = runQuery(request)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(330L)));
                assertNotNull(resp.profile());
                List<DriverProfile> drivers = resp.profile().drivers();
                assertThat(drivers.size(), greaterThanOrEqualTo(4)); // two coordinators and at least two data
                allProfiles = drivers.size();

                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertNotNull(executionInfo);
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), is(false));
                long overallTookMillis = executionInfo.overallTook().millis();
                assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertClusterInfoSuccess(remoteCluster, remoteNumShards, overallTookMillis);

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);
            }
        }
        assertThat(allProfiles, equalTo(localOnlyProfiles + remoteOnlyProfiles - 1));
    }

    public void testWarnings() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM logs-*,c*:logs-* | EVAL ip = to_ip(id) | STATS total = sum(v) by ip | LIMIT 10"
        );
        InternalTestCluster cluster = cluster(LOCAL_CLUSTER);
        String node = randomFrom(cluster.getNodeNames());
        CountDownLatch latch = new CountDownLatch(1);
        cluster.client(node).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(resp -> {
            TransportService ts = cluster.getInstance(TransportService.class, node);
            Map<String, List<String>> responseHeaders = ts.getThreadPool().getThreadContext().getResponseHeaders();
            List<String> warnings = responseHeaders.getOrDefault("Warning", List.of())
                .stream()
                .filter(w -> w.contains("is not an IP string literal"))
                .toList();
            assertThat(warnings.size(), greaterThanOrEqualTo(20));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0).get(0), equalTo(330L));
            assertNull(values.get(0).get(1));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.includeCCSMetadata(), is(false));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterInfoSuccess(remoteCluster, remoteNumShards, overallTookMillis);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, localNumShards, overallTookMillis);

            latch.countDown();
        }, e -> {
            latch.countDown();
            throw new AssertionError(e);
        }));
        assertTrue(latch.await(30, TimeUnit.SECONDS));
    }

    // Non-disconnect remote failures lead to skipping if skip_unavailable is true
    public void testRemoteFailureSkipUnavailableTrue() throws IOException {
        Map<String, Object> testClusterInfo = setupFailClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remote1Index = (String) testClusterInfo.get("remote.index");
        String q = Strings.format("FROM %s,cluster-a:%s*", localIndex, remote1Index);

        try (EsqlQueryResponse resp = runQuery(q, randomBoolean())) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs-2*"));
            assertClusterInfoSkipped(remoteCluster);
            assertThat(remoteCluster.getFailures().getFirst().reason(), containsString("Accessing failing field"));
        }
    }

    public void testRemoteFailureInlinestats() throws IOException {
        assumeTrue("requires inlinestats", INLINE_STATS_SUPPORTS_REMOTE.isEnabled());
        Map<String, Object> testClusterInfo = setupFailClusters();
        String localIndex = (String) testClusterInfo.get("local.index");
        String remote1Index = (String) testClusterInfo.get("remote.index");
        // This will fail in the main plan
        String q = Strings.format("FROM %s,cluster-a:%s* | INLINE STATS SUM(v) | SORT v", localIndex, remote1Index);

        try (EsqlQueryResponse resp = runQuery(q, randomBoolean())) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs-2*"));
            assertClusterInfoSkipped(remoteCluster);
            assertThat(remoteCluster.getFailures().getFirst().reason(), containsString("Accessing failing field"));
        }
        // This will fail in the INLINE STATS subplan, skipping should still work the same
        q = Strings.format("FROM cluster-a:%s* | INLINE STATS SUM(fail_me) | SORT fail_me", remote1Index);

        try (EsqlQueryResponse resp = runQuery(q, randomBoolean())) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs-2*"));
            assertClusterInfoSkipped(remoteCluster);
            assertThat(remoteCluster.getFailures().getFirst().reason(), containsString("Accessing failing field"));
        }
    }

    private static void assertClusterMetadataInResponse(EsqlQueryResponse resp, boolean responseExpectMeta) {
        try {
            final Map<String, Object> esqlResponseAsMap = XContentTestUtils.convertToMap(resp);
            final Object clusters = esqlResponseAsMap.get("_clusters");
            if (responseExpectMeta) {
                assertNotNull(clusters);
                // test a few entries to ensure it looks correct (other tests do a full analysis of the metadata in the response)
                @SuppressWarnings("unchecked")
                Map<String, Object> inner = (Map<String, Object>) clusters;
                assertTrue(inner.containsKey("total"));
                assertTrue(inner.containsKey("details"));
            } else {
                assertNull(clusters);
            }
        } catch (IOException e) {
            fail("Could not convert ESQL response to Map: " + e);
        }
    }

    void waitForNoInitializingShards(Client client, TimeValue timeout, String... indices) {
        ClusterHealthResponse resp = client.admin()
            .cluster()
            .prepareHealth(TEST_REQUEST_TIMEOUT, indices)
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .setTimeout(timeout)
            .get();
        assertFalse(Strings.toString(resp, true, true), resp.isTimedOut());
    }

    Map<String, Object> setupTwoClusters() throws IOException {
        return setupClusters(2);
    }

    /**
     * For the local cluster and REMOTE_CLUSTER_1 it creates a standard alias to the index created in populateLocalIndices
     * and populateRemoteIndices. It also creates a filtered alias against those indices that looks like:
     * PUT /_aliases
     * {
     *   "actions": [
     *     {
     *       "add": {
     *         "index": "my_index",
     *         "alias": "my_alias",
     *         "filter": {
     *           "terms": {
     *             "v": [1, 2, 4]
     *           }
     *         }
     *       }
     *     }
     *   ]
     * }
     */
    void createIndexAliases(int numClusters) {
        assert numClusters == 2 || numClusters == 3 : "Only 2 or 3 clusters allowed in createIndexAliases";

        int[] allowed = new int[] { 1, 2, 4 };
        QueryBuilder filterBuilder = new TermsQueryBuilder("v", allowed);

        {
            Client localClient = client(LOCAL_CLUSTER);
            IndicesAliasesResponse indicesAliasesResponse = localClient.admin()
                .indices()
                .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias(LOCAL_INDEX, IDX_ALIAS)
                .addAlias(LOCAL_INDEX, FILTERED_IDX_ALIAS, filterBuilder)
                .get();
            assertFalse(indicesAliasesResponse.hasErrors());
        }
        {
            Client remoteClient = client(REMOTE_CLUSTER_1);
            IndicesAliasesResponse indicesAliasesResponse = remoteClient.admin()
                .indices()
                .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias(REMOTE_INDEX, IDX_ALIAS)
                .addAlias(REMOTE_INDEX, FILTERED_IDX_ALIAS, filterBuilder)
                .get();
            assertFalse(indicesAliasesResponse.hasErrors());
        }
        if (numClusters == 3) {
            Client remoteClient = client(REMOTE_CLUSTER_2);
            IndicesAliasesResponse indicesAliasesResponse = remoteClient.admin()
                .indices()
                .prepareAliases(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .addAlias(REMOTE_INDEX, IDX_ALIAS)
                .addAlias(REMOTE_INDEX, FILTERED_IDX_ALIAS, filterBuilder)
                .get();
            assertFalse(indicesAliasesResponse.hasErrors());
        }
    }

    Map<String, String> createEmptyIndicesWithNoMappings(int numClusters) {
        assert numClusters == 2 || numClusters == 3 : "Only 2 or 3 clusters supported in createEmptyIndicesWithNoMappings";

        Map<String, String> clusterToEmptyIndexMap = new HashMap<>();

        String localIndexName = randomAlphaOfLength(14).toLowerCase(Locale.ROOT) + "1";
        clusterToEmptyIndexMap.put(LOCAL_CLUSTER, localIndexName);
        Client localClient = client(LOCAL_CLUSTER);
        assertAcked(
            localClient.admin().indices().prepareCreate(localIndexName).setSettings(Settings.builder().put("index.number_of_shards", 1))
        );

        String remote1IndexName = randomAlphaOfLength(14).toLowerCase(Locale.ROOT) + "2";
        clusterToEmptyIndexMap.put(REMOTE_CLUSTER_1, remote1IndexName);
        Client remote1Client = client(REMOTE_CLUSTER_1);
        assertAcked(
            remote1Client.admin().indices().prepareCreate(remote1IndexName).setSettings(Settings.builder().put("index.number_of_shards", 1))
        );

        if (numClusters == 3) {
            String remote2IndexName = randomAlphaOfLength(14).toLowerCase(Locale.ROOT) + "3";
            clusterToEmptyIndexMap.put(REMOTE_CLUSTER_2, remote2IndexName);
            Client remote2Client = client(REMOTE_CLUSTER_2);
            assertAcked(
                remote2Client.admin()
                    .indices()
                    .prepareCreate(remote2IndexName)
                    .setSettings(Settings.builder().put("index.number_of_shards", 1))
            );
        }

        return clusterToEmptyIndexMap;
    }

    Map<String, Object> setupFailClusters() throws IOException {
        int numShardsLocal = randomIntBetween(1, 3);
        populateIndex(LOCAL_CLUSTER, LOCAL_INDEX, numShardsLocal, 10);

        int numShardsRemote = randomIntBetween(1, 3);
        populateRemoteIndicesFail(REMOTE_CLUSTER_1, REMOTE_INDEX, numShardsRemote);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", LOCAL_INDEX);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", REMOTE_INDEX);
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        return clusterInfo;
    }

    void populateRemoteIndicesFail(String clusterAlias, String indexName, int numShards) throws IOException {
        Client remoteClient = client(clusterAlias);
        XContentBuilder mapping = JsonXContent.contentBuilder().startObject();
        mapping.startObject("runtime");
        {
            mapping.startObject("fail_me");
            {
                mapping.field("type", "long");
                mapping.startObject("script").field("source", "").field("lang", FailingFieldPlugin.FAILING_FIELD_LANG).endObject();
            }
            mapping.endObject();
        }
        mapping.endObject();
        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping(mapping.endObject())
        );

        remoteClient.prepareIndex(indexName).setSource("id", 0).get();
        remoteClient.admin().indices().prepareRefresh(indexName).get();
    }

    public void testMultiTypes() throws Exception {
        Client remoteClient = client(REMOTE_CLUSTER_1);
        int totalDocs = 0;
        for (String type : List.of("integer", "long")) {
            String index = "conflict-index-" + type;
            assertAcked(remoteClient.admin().indices().prepareCreate(index).setMapping("port", "type=" + type));
            int numDocs = between(1, 10);
            for (int i = 0; i < numDocs; i++) {
                remoteClient.prepareIndex(index).setId(Integer.toString(i)).setSource("port", i).get();
            }
            remoteClient.admin().indices().prepareRefresh(index).get();
            totalDocs += numDocs;
        }
        for (String castFunction : List.of("TO_LONG", "TO_INT")) {
            EsqlQueryRequest request = new EsqlQueryRequest();
            request.query("FROM *:conflict-index-* | EVAL port=" + castFunction + "(port) | WHERE port is NOT NULL | STATS COUNT(port)");
            try (EsqlQueryResponse resp = runQuery(request)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values, hasSize(1));
                assertThat(values.get(0), hasSize(1));
                assertThat(values.get(0).get(0), equalTo((long) totalDocs));
            }
        }
    }

    public void testNoBothIncludeCcsMetadataAndIncludeExecutionMetadata() throws Exception {
        setupTwoClusters();
        var query = "from logs-*,c*:logs-* | stats sum (v)";
        EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(AbstractEsqlIntegTestCase.randomPragmas())
            .profile(randomInt(5) == 2)
            .columnar(randomBoolean())
            .includeCCSMetadata(randomBoolean())
            .includeExecutionMetadata(randomBoolean());

        assertThat(
            expectThrows(VerificationException.class, () -> runQuery(request)).getMessage(),
            containsString("Both [include_execution_metadata] and [include_ccs_metadata] query parameters are set. Use only one")
        );
    }

    /**
     * Test EXPLAIN with CCS (Cross-Cluster Search) to verify that local plans are fetched from remote clusters.
     */
    public void testExplainCCS() throws Exception {
        assumeTrue("EXPLAIN requires the capability to be enabled", EXPLAIN.isEnabled());
        setupTwoClusters();

        // Run EXPLAIN query that targets both local and remote clusters
        String explainQuery = "EXPLAIN (FROM logs-*," + REMOTE_CLUSTER_1 + ":logs-* | STATS count = COUNT(*) BY tag)";

        try (EsqlQueryResponse results = runQuery(explainQuery, false)) {
            // Verify the columns are correct
            assertThat(
                results.columns(),
                equalTo(
                    List.of(
                        new ColumnInfoImpl("cluster", "keyword", null),
                        new ColumnInfoImpl("node", "keyword", null),
                        new ColumnInfoImpl("role", "keyword", null),
                        new ColumnInfoImpl("type", "keyword", null),
                        new ColumnInfoImpl("plan", "keyword", null)
                    )
                )
            );

            // Verify we have rows with plan information
            List<List<Object>> values = getValuesList(results);
            assertThat(values.size(), greaterThanOrEqualTo(3));

            // Check that we have coordinator plans and plans from remote cluster
            boolean hasParsedPlan = false;
            boolean hasOptimizedLogicalPlan = false;
            boolean hasOptimizedPhysicalPlan = false;
            boolean hasLocalPhysicalPlan = false;
            boolean hasRemoteClusterPlan = false;

            for (List<Object> row : values) {
                String cluster = (String) row.get(0);
                String node = (String) row.get(1);
                String role = (String) row.get(2);
                String type = (String) row.get(3);
                String plan = (String) row.get(4);

                assertNotNull(plan);
                assertNotNull(role);
                assertNotNull(type);

                if ("coordinator".equals(role) && "parsedPlan".equals(type)) {
                    hasParsedPlan = true;
                    // Local cluster plans should have empty cluster name
                    assertThat(cluster, equalTo(""));
                }
                if ("coordinator".equals(role) && "optimizedLogicalPlan".equals(type)) {
                    hasOptimizedLogicalPlan = true;
                }
                if ("coordinator".equals(role) && "optimizedPhysicalPlan".equals(type)) {
                    hasOptimizedPhysicalPlan = true;
                }
                if ("data".equals(role) && "localPhysicalPlan".equals(type)) {
                    hasLocalPhysicalPlan = true;
                    // The local physical plan should contain an Elasticsearch execution node
                    assertThat(
                        "Local physical plan should contain an Es*Exec node",
                        plan,
                        anyOf(containsString("EsQueryExec"), containsString("EsStatsQueryExec"))
                    );
                    // Should not contain FragmentExec - that should be mapped to concrete operators
                    assertThat("Local physical plan should not contain FragmentExec", plan, not(containsString("FragmentExec")));
                    // Check if this is from remote cluster
                    if (REMOTE_CLUSTER_1.equals(cluster)) {
                        hasRemoteClusterPlan = true;
                        assertThat(node, is(org.hamcrest.Matchers.not(org.hamcrest.Matchers.nullValue())));
                    }
                }
            }

            assertThat("Should have parsed plan", hasParsedPlan, is(true));
            assertThat("Should have optimized logical plan", hasOptimizedLogicalPlan, is(true));
            assertThat("Should have optimized physical plan", hasOptimizedPhysicalPlan, is(true));
            assertThat("Should have local physical plan", hasLocalPhysicalPlan, is(true));
            assertThat("Should have plan from remote cluster " + REMOTE_CLUSTER_1, hasRemoteClusterPlan, is(true));
        }
    }

    /**
     * Test EXPLAIN with CCS where a filter references a field that only exists on the local cluster.
     * This demonstrates different local plans for different clusters based on field availability:
     * - Local cluster: EsRelation (field exists, query proceeds normally)
     * - Remote cluster: LocalRelation (field doesn't exist, optimizer prunes to empty result)
     */
    public void testExplainCCSWithFieldOnlyOnLocalCluster() throws Exception {
        assumeTrue("EXPLAIN requires the capability to be enabled", EXPLAIN.isEnabled());

        // First set up the standard cluster infrastructure
        setupTwoClusters();

        String localIndex = "test-local-field-index";
        String remoteIndex = "test-remote-no-field-index";

        // Create index on local cluster WITH the special field
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(localIndex)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("common_field", "type=keyword", "local_only_field", "type=keyword")
        );
        client(LOCAL_CLUSTER).prepareIndex(localIndex).setSource("common_field", "value1", "local_only_field", "local_value").get();
        client(LOCAL_CLUSTER).admin().indices().prepareRefresh(localIndex).get();

        // Wait for green status on the local index
        client(LOCAL_CLUSTER).admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT, localIndex).setWaitForGreenStatus().get();

        // Create index on remote cluster WITHOUT the special field
        Client remoteClient = client(REMOTE_CLUSTER_1);
        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate(remoteIndex)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("common_field", "type=keyword")
        );
        remoteClient.prepareIndex(remoteIndex).setSource("common_field", "value2").get();
        remoteClient.admin().indices().prepareRefresh(remoteIndex).get();

        // Wait for green status on the remote index
        cluster(REMOTE_CLUSTER_1).ensureAtLeastNumDataNodes(1);
        remoteClient.admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex).setWaitForGreenStatus().get();

        // The query to test - filters on a field that only exists on local cluster
        String baseQuery = "FROM "
            + localIndex
            + ","
            + REMOTE_CLUSTER_1
            + ":"
            + remoteIndex
            + " | WHERE local_only_field == \"test\" | KEEP common_field";

        // Step 1: Run the query with profile=true to get actual execution plans
        EsqlQueryRequest profileRequest = syncEsqlQueryRequest(baseQuery);
        profileRequest.profile(true);

        String profiledLocalPlan = null;
        String profiledRemotePlan = null;

        try (EsqlQueryResponse profiledResponse = runQuery(profileRequest)) {
            assertNotNull("Profile should be present", profiledResponse.profile());

            // Extract plans by cluster from profile
            for (var planProfile : profiledResponse.profile().plans()) {
                if (planProfile.description().contains("data")) {
                    // Local cluster has empty cluster name
                    if (planProfile.clusterName().isEmpty() || planProfile.clusterName().equals("main-cluster")) {
                        profiledLocalPlan = planProfile.planTree();
                    } else if (planProfile.clusterName().equals(REMOTE_CLUSTER_1)) {
                        profiledRemotePlan = planProfile.planTree();
                    }
                }
            }
        }

        // Step 2: Run EXPLAIN with a filter on the field that only exists locally
        String explainQuery = "EXPLAIN (" + baseQuery + ")";

        try (EsqlQueryResponse results = runQuery(explainQuery, false)) {
            List<List<Object>> values = getValuesList(results);

            // Track plans by cluster from EXPLAIN
            // Note: localLogicalPlan is not yet implemented in profile capture, only localPhysicalPlan is available
            String explainLocalPhysicalPlan = null;
            String explainRemotePhysicalPlan = null;

            for (List<Object> row : values) {
                String cluster = (String) row.get(0);
                String role = (String) row.get(2);
                String type = (String) row.get(3);
                String plan = (String) row.get(4);

                if ("data".equals(role) && "localPhysicalPlan".equals(type)) {
                    if ("".equals(cluster)) {
                        explainLocalPhysicalPlan = plan;
                    } else if (REMOTE_CLUSTER_1.equals(cluster)) {
                        explainRemotePhysicalPlan = plan;
                    }
                }
            }

            // Assertions for local cluster (field exists)
            if (explainLocalPhysicalPlan != null) {
                assertThat(
                    "Local cluster physical plan should have EsQueryExec (field exists)",
                    explainLocalPhysicalPlan,
                    anyOf(containsString("EsQueryExec"), containsString("EsStatsQueryExec"))
                );

                // Compare with profiled plan if available
                if (profiledLocalPlan != null) {
                    List<String> profiledOperators = extractOperators(profiledLocalPlan);
                    List<String> explainOperators = extractOperators(explainLocalPhysicalPlan);

                    // Remove ExchangeSinkExec wrapper if present in either plan
                    if (profiledOperators.size() > 0 && profiledOperators.get(0).equals("ExchangeSinkExec")) {
                        profiledOperators = profiledOperators.subList(1, profiledOperators.size());
                    }
                    if (explainOperators.size() > 0 && explainOperators.get(0).equals("ExchangeSinkExec")) {
                        explainOperators = explainOperators.subList(1, explainOperators.size());
                    }

                    assertThat(
                        "EXPLAIN local physical plan should have same operators as profiled plan for local cluster",
                        explainOperators,
                        equalTo(profiledOperators)
                    );
                }
            }

            // Assertions for remote cluster (field doesn't exist, pruned)
            // The physical plan should show LocalSourceExec indicating the query was pruned
            assertNotNull("Should have local physical plan for remote cluster", explainRemotePhysicalPlan);
            assertThat(
                "Remote cluster physical plan should have LocalSourceExec (pruned to empty)",
                explainRemotePhysicalPlan,
                containsString("LocalSourceExec")
            );

            // Compare remote cluster plans with profile if available
            if (profiledRemotePlan != null) {
                List<String> profiledOperators = extractOperators(profiledRemotePlan);
                List<String> explainOperators = extractOperators(explainRemotePhysicalPlan);

                // Remove ExchangeSinkExec wrapper if present in either plan
                if (profiledOperators.size() > 0 && profiledOperators.get(0).equals("ExchangeSinkExec")) {
                    profiledOperators = profiledOperators.subList(1, profiledOperators.size());
                }
                if (explainOperators.size() > 0 && explainOperators.get(0).equals("ExchangeSinkExec")) {
                    explainOperators = explainOperators.subList(1, explainOperators.size());
                }

                assertThat(
                    "EXPLAIN local physical plan should have same operators as profiled plan for remote cluster",
                    explainOperators,
                    equalTo(profiledOperators)
                );
            }
        }
    }

    /**
     * Test EXPLAIN with INLINE STATS in a cross-cluster query.
     * INLINE STATS creates a subplan that is executed separately, and EXPLAIN should show this.
     *
     * Note: EXPLAIN with INLINE STATS currently only returns coordinator-level plans (not data node plans).
     * This is because InlineJoin contains StubRelation which cannot be serialized for remote execution.
     * In normal execution, subplans are executed first and their results replace StubRelation.
     * For EXPLAIN, we skip remote planning and only show coordinator plans plus subplan information.
     */
    public void testExplainCCSWithInlineStats() throws Exception {
        assumeTrue("EXPLAIN requires the capability to be enabled", EXPLAIN.isEnabled());
        assumeTrue("INLINE STATS requires the capability to be enabled", INLINE_STATS.isEnabled());
        assumeTrue("INLINE STATS with remote requires the capability to be enabled", INLINE_STATS_SUPPORTS_REMOTE.isEnabled());

        // Set up the standard cluster infrastructure
        setupTwoClusters();

        String localIndex = "test-inline-stats-local";
        String remoteIndex = "test-inline-stats-remote";

        // Create index on local cluster
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .indices()
                .prepareCreate(localIndex)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("category", "type=keyword", "value", "type=integer")
        );
        client(LOCAL_CLUSTER).prepareIndex(localIndex).setSource("category", "A", "value", 10).get();
        client(LOCAL_CLUSTER).prepareIndex(localIndex).setSource("category", "A", "value", 20).get();
        client(LOCAL_CLUSTER).prepareIndex(localIndex).setSource("category", "B", "value", 30).get();
        client(LOCAL_CLUSTER).admin().indices().prepareRefresh(localIndex).get();
        client(LOCAL_CLUSTER).admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT, localIndex).setWaitForGreenStatus().get();

        // Create index on remote cluster
        Client remoteClient = client(REMOTE_CLUSTER_1);
        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate(remoteIndex)
                .setSettings(Settings.builder().put("index.number_of_shards", 1))
                .setMapping("category", "type=keyword", "value", "type=integer")
        );
        remoteClient.prepareIndex(remoteIndex).setSource("category", "A", "value", 40).get();
        remoteClient.prepareIndex(remoteIndex).setSource("category", "B", "value", 50).get();
        remoteClient.admin().indices().prepareRefresh(remoteIndex).get();
        cluster(REMOTE_CLUSTER_1).ensureAtLeastNumDataNodes(1);
        remoteClient.admin().cluster().prepareHealth(TEST_REQUEST_TIMEOUT, remoteIndex).setWaitForGreenStatus().get();

        // Query with INLINE STATS - this creates a subplan for the aggregation
        String baseQuery = "FROM "
            + localIndex
            + ","
            + REMOTE_CLUSTER_1
            + ":"
            + remoteIndex
            + " | INLINE STATS total = SUM(value) BY category | KEEP category, value, total | SORT category, value";

        // Run EXPLAIN - for INLINE STATS, we only get coordinator plans (no data node plans)
        String explainQuery = "EXPLAIN (" + baseQuery + ")";

        try (EsqlQueryResponse results = runQuery(explainQuery, false)) {
            List<List<Object>> values = getValuesList(results);

            // Track plans from EXPLAIN
            String explainParsedPlan = null;
            String explainOptimizedLogicalPlan = null;
            String explainOptimizedPhysicalPlan = null;
            String explainSubplanLogical = null;
            String explainSubplanPhysical = null;
            boolean hasDataNodePlans = false;

            for (List<Object> row : values) {
                String role = (String) row.get(2);
                String type = (String) row.get(3);
                String plan = (String) row.get(4);

                if ("coordinator".equals(role)) {
                    if ("parsedPlan".equals(type)) {
                        explainParsedPlan = plan;
                    } else if ("optimizedLogicalPlan".equals(type)) {
                        explainOptimizedLogicalPlan = plan;
                    } else if ("optimizedPhysicalPlan".equals(type)) {
                        explainOptimizedPhysicalPlan = plan;
                    }
                } else if (role.startsWith("subplan-")) {
                    if ("logicalPlan".equals(type)) {
                        explainSubplanLogical = plan;
                    } else if ("physicalPlan".equals(type)) {
                        explainSubplanPhysical = plan;
                    }
                } else if ("data".equals(role)) {
                    hasDataNodePlans = true;
                }
            }

            // Verify coordinator plans are present
            assertNotNull("Should have parsed plan", explainParsedPlan);
            assertNotNull("Should have optimized logical plan", explainOptimizedLogicalPlan);
            assertNotNull("Should have optimized physical plan", explainOptimizedPhysicalPlan);

            // Verify the optimized logical plan contains InlineJoin (INLINE STATS is transformed to InlineJoin)
            assertThat(
                "Optimized logical plan should contain InlineJoin (from INLINE STATS)",
                explainOptimizedLogicalPlan,
                containsString("InlineJoin")
            );

            // Verify subplan information is present
            assertNotNull("Should have subplan logical plan", explainSubplanLogical);
            assertNotNull("Should have subplan physical plan", explainSubplanPhysical);

            // Verify subplan logical plan contains the aggregation components:
            // - Aggregate node for the INLINE STATS aggregation
            // - SUM function for the "total = SUM(value)" expression
            // - Grouping by "category"
            assertThat("Subplan logical plan should contain Aggregate", explainSubplanLogical, containsString("Aggregate"));
            assertThat("Subplan logical plan should contain SUM aggregation", explainSubplanLogical, containsString("SUM"));
            assertThat("Subplan logical plan should group by category", explainSubplanLogical, containsString("category"));

            // Verify subplan physical plan contains execution operators
            assertThat(
                "Subplan physical plan should contain aggregation execution",
                explainSubplanPhysical,
                anyOf(containsString("AggregateExec"), containsString("HashAggregation"))
            );

            // With the unified execution path, INLINE STATS queries now show full execution plans
            // including data node plans from subplan execution. This is an improvement over the
            // previous limitation where subplans prevented data node plan capture.
            assertTrue("EXPLAIN with INLINE STATS should now include data node plans", hasDataNodePlans);
        }
    }

    /**
     * Extract operator names from a plan string in order.
     * Operators are identified by the pattern "OperatorExec[" in the plan string.
     */
    private List<String> extractOperators(String plan) {
        List<String> operators = new ArrayList<>();
        Pattern pattern = Pattern.compile("([A-Z][a-zA-Z]+Exec)\\[");
        Matcher matcher = pattern.matcher(plan);
        while (matcher.find()) {
            operators.add(matcher.group(1));
        }
        return operators;
    }
}
