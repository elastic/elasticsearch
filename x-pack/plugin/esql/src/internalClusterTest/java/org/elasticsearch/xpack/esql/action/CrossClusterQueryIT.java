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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.operator.DriverProfile;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CrossClusterQueryIT extends AbstractCrossClusterTestCase {
    private static final String IDX_ALIAS = "alias1";
    private static final String FILTERED_IDX_ALIAS = "alias-filtered-1";

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean(), REMOTE_CLUSTER_2, randomBoolean());
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
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));

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
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));

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
            // TODO bug - this does not throw; uncomment this test once https://github.com/elastic/elasticsearch/issues/114495 is fixed
            // String limit0 = q + " | LIMIT 0";
            // VerificationException ve = expectThrows(VerificationException.class, () -> runQuery(limit0, false));
            // assertThat(ve.getDetailedMessage(), containsString("No matching indices for [nomatch]"));
        }

        {
            // no failure since concrete index matches, so wildcard matching is lenient
            String q = "FROM nomatch*," + localIndex;
            try (EsqlQueryResponse resp = runQuery(q, false)) {
                // we are only testing that this does not throw an Exception, so the asserts below are minimal
                assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(false));
            }

            String limit0 = q + " | LIMIT 0";
            try (EsqlQueryResponse resp = runQuery(limit0, false)) {
                // we are only testing that this does not throw an Exception, so the asserts below are minimal
                assertThat(resp.columns().size(), greaterThanOrEqualTo(1));
                assertThat(getValuesList(resp).size(), equalTo(0));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(false));
            }
        }
        {
            String q = "FROM nomatch";
            String expectedError = "Unknown index [nomatch]";
            expectVerificationExceptionForQuery(q, expectedError, false);
        }
        {
            String q = "FROM nomatch*";
            String expectedError = "Unknown index [nomatch*]";
            expectVerificationExceptionForQuery(q, expectedError, false);
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

        // an error is thrown if there is a concrete index that does not match
        {
            String q = "FROM cluster-a:nomatch";
            String expectedError = "Unknown index [cluster-a:nomatch]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }

        // an error is thrown if there are no matching indices at all - single remote cluster with wildcard index expression
        {
            String q = "FROM cluster-a:nomatch*";
            String expectedError = "Unknown index [cluster-a:nomatch*]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }

        // an error is thrown if there is a concrete index that does not match
        {
            String q = "FROM nomatch*,cluster-a:nomatch";
            String expectedError = "Unknown index [cluster-a:nomatch,nomatch*]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }

        // an error is thrown if there are no matching indices at all - local with wildcard, remote with wildcard
        {
            String q = "FROM nomatch*,cluster-a:nomatch*";
            String expectedError = "Unknown index [cluster-a:nomatch*,nomatch*]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }
        {
            String q = "FROM nomatch,cluster-a:nomatch";
            String expectedError = "Unknown index [cluster-a:nomatch,nomatch]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }
        {
            String q = "FROM nomatch,cluster-a:nomatch*";
            String expectedError = "Unknown index [cluster-a:nomatch*,nomatch]";
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
    private void expectVerificationExceptionForQuery(String query, String error, Boolean requestIncludeMeta) {
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
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*,no_such_index*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));
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
        try (EsqlQueryResponse resp = runQuery("FROM logs*,cluster-a:* | LIMIT 0", requestIncludeMeta)) {
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
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));
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
                Strings.format("FROM logs*,%s:logs* METADATA _index | stats sum(v) by _index | sort _index", REMOTE_CLUSTER_1),
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
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(executionInfo.isPartial(), equalTo(false));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));
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
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            request.query("FROM logs* | stats sum(v)");
            request.pragmas(pragmas);
            request.profile(true);
            try (EsqlQueryResponse resp = runQuery(request)) {
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
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            request.query("FROM c*:logs* | stats sum(v)");
            request.pragmas(pragmas);
            request.profile(true);
            try (EsqlQueryResponse resp = runQuery(request)) {
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
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs*"));
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
                assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
                assertThat(remoteCluster.getSkippedShards(), equalTo(0));
                assertThat(remoteCluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertNull(localCluster);
            }
        }
        final int allProfiles;
        {
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            request.query("FROM logs*,c*:logs* | stats total = sum(v)");
            request.pragmas(pragmas);
            request.profile(true);
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
                assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));

                EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
                assertThat(remoteCluster.getIndexExpression(), equalTo("logs*"));
                assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
                assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
                assertThat(remoteCluster.getSkippedShards(), equalTo(0));
                assertThat(remoteCluster.getFailedShards(), equalTo(0));

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
                assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
                assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
                assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
                assertThat(localCluster.getSkippedShards(), equalTo(0));
                assertThat(localCluster.getFailedShards(), equalTo(0));
            }
        }
        assertThat(allProfiles, equalTo(localOnlyProfiles + remoteOnlyProfiles - 1));
    }

    public void testWarnings() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query("FROM logs*,c*:logs* | EVAL ip = to_ip(id) | STATS total = sum(v) by ip | LIMIT 10");
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
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTotalShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(remoteNumShards));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(localCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(localCluster.getTotalShards(), equalTo(localNumShards));
            assertThat(localCluster.getSuccessfulShards(), equalTo(localNumShards));
            assertThat(localCluster.getSkippedShards(), equalTo(0));
            assertThat(localCluster.getFailedShards(), equalTo(0));

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
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remoteCluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remoteCluster.getTotalShards(), equalTo(0));
            assertThat(remoteCluster.getSuccessfulShards(), equalTo(0));
            assertThat(remoteCluster.getSkippedShards(), equalTo(0));
            assertThat(remoteCluster.getFailedShards(), equalTo(0));
            assertThat(remoteCluster.getFailures(), hasSize(1));
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
}
