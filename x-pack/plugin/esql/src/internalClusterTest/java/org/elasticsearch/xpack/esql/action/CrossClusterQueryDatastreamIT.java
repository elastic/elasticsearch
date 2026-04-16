/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesResponse;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.datastreams.GetDataStreamAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStore;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.lucene.query.DataPartitioning;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CrossClusterQueryDatastreamIT extends AbstractCrossClusterTestCase {
    protected static final String IDX_ALIAS = "alias1";
    protected static final String FILTERED_IDX_ALIAS = "alias-filtered-1";

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, false, REMOTE_CLUSTER_2, false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return CollectionUtils.concatLists(List.of(MapperExtrasPlugin.class, DataStreamsPlugin.class), super.nodePlugins(clusterAlias));
    }

    protected void assertClusterInfoSuccess(EsqlExecutionInfo.Cluster clusterInfo, int numShards, long overallTookMillis) {
        assertClusterInfoSuccess(clusterInfo, "logs-*", numShards, overallTookMillis);
    }

    protected void assertClusterInfoSuccess(
        EsqlExecutionInfo.Cluster clusterInfo,
        String indexExpression,
        int numShards,
        long overallTookMillis
    ) {
        assertThat(clusterInfo.getIndexExpression(), equalTo(indexExpression));
        assertThat(clusterInfo.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
        super.assertClusterInfoSuccess(clusterInfo, numShards);
    }

    private void resetSkipUnavailableDefaults() {
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);
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

    public void testSuccessfulPathwaysFailureStore() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();
        try (
            EsqlQueryResponse resp = runQuery("from logs-*::failures,c*:logs-*::failures | stats count (error.type)", requestIncludeMeta)
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(10L)));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterInfoSuccess(remoteCluster, "logs-*::failures", remoteNumShards, overallTookMillis);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, "logs-*::failures", localNumShards, overallTookMillis);

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
            try (EsqlQueryResponse resp = runQuery(q, false)) {
                assertThat(getValuesList(resp).size(), equalTo(0));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(false));
            }
        }
    }

    public void testSearchesAgainstNonMatchingFailureStoreIndicesWithLocalOnly() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        String localIndex = (String) testClusterInfo.get("local.index");

        {
            String q = "FROM nomatch::failures," + localIndex + "::failures";
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
            String q = "FROM nomatch*::failures," + localIndex + "::failures";
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
            String q = "FROM nomatch::failures";
            String expectedError = "Unknown index [nomatch::failures]";
            expectVerificationExceptionForQuery(q, expectedError, false);
        }
        {
            String q = "FROM nomatch*::failures";
            try (EsqlQueryResponse resp = runQuery(q, false)) {
                assertThat(getValuesList(resp).size(), equalTo(0));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(false));
            }
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

        createDataStreamAliases(numClusters);

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
            try {
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
            } finally {
                resetSkipUnavailableDefaults();
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
            try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), equalTo(0));
            }
        }

        // an error is thrown if there is a concrete index that does not match
        {
            String q = "FROM nomatch*,cluster-a:nomatch";
            String expectedError = "Unknown index [cluster-a:nomatch]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }

        // an error is thrown if there are no matching indices at all - local with wildcard, remote with wildcard
        {
            String q = "FROM nomatch*,cluster-a:nomatch*";
            try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), equalTo(0));
            }
        }
        {
            String q = "FROM nomatch,cluster-a:nomatch";
            String expectedError = "Unknown index [cluster-a:nomatch]; Unknown index [nomatch]";
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
            try {
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
            } finally {
                resetSkipUnavailableDefaults();
            }
        }
    }

    public void testSearchesAgainstNonMatchingFailureStoreIndices() throws Exception {
        int numClusters = 3;
        Map<String, Object> testClusterInfo = setupClusters(numClusters);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote1NumShards = (Integer) testClusterInfo.get("remote1.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");
        String localIndex = (String) testClusterInfo.get("local.index");
        String remote1Index = (String) testClusterInfo.get("remote1.index");
        String remote2Index = (String) testClusterInfo.get("remote2.index");

        createDataStreamAliases(numClusters);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // missing concrete local index is an error
        {
            String q = "FROM nomatch::failures,cluster-a:" + remote1Index + "::failures";
            String expectedError = "Unknown index [nomatch::failures]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }

        // missing concrete remote index is fatal
        {
            String q = "FROM logs*::failures,cluster-a:nomatch::failures";
            String expectedError = "Unknown index [cluster-a:nomatch::failures]";
            try {
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
                                "logs*::failures",
                                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                                localNumShards
                            ),
                            new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch::failures", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0)
                        )
                    );

                }
            } finally {
                resetSkipUnavailableDefaults();
            }
        }

        // No error since local non-matching index has wildcard and the remote cluster index expression matches
        {
            String remote1IndexName = randomFrom(remote1Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
            String q = Strings.format("FROM nomatch*::failures,%s:%s::failures", REMOTE_CLUSTER_1, remote1IndexName);
            try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertExpectedClustersForMissingIndicesTests(
                    executionInfo,
                    List.of(
                        // local cluster is never marked as SKIPPED even when no matcing indices - just marked as 0 shards searched
                        new ExpectedCluster(LOCAL_CLUSTER, "nomatch*::failures", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                        new ExpectedCluster(
                            REMOTE_CLUSTER_1,
                            remote1IndexName + "::failures",
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
                        new ExpectedCluster(LOCAL_CLUSTER, "nomatch*::failures", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                        // LIMIT 0 searches always have total shards = 0
                        new ExpectedCluster(
                            REMOTE_CLUSTER_1,
                            remote1IndexName + "::failures",
                            EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                            0
                        )
                    )
                );
            }
        }

        // No error since remote non-matching index has wildcard and the local cluster index expression matches
        {
            String indexLoc = randomFrom(localIndex, IDX_ALIAS, FILTERED_IDX_ALIAS);
            String q = Strings.format("FROM %s::failures,cluster-a:nomatch*::failures", indexLoc);

            try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                assertThat(executionInfo.isCrossClusterSearch(), is(true));
                assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                assertExpectedClustersForMissingIndicesTests(
                    executionInfo,
                    List.of(
                        // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                        new ExpectedCluster(
                            LOCAL_CLUSTER,
                            indexLoc + "::failures",
                            EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                            localNumShards
                        ),
                        new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch*::failures", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0)
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
                        new ExpectedCluster(LOCAL_CLUSTER, indexLoc + "::failures", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                        new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch*::failures", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0)
                    )
                );
            }
        }

        // an error is thrown if there is a concrete index that does not match
        {
            String q = "FROM cluster-a:nomatch::failures";
            String expectedError = "Unknown index [cluster-a:nomatch::failures]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }

        // no error on wildcard non-matching remote index
        {
            String q = "FROM cluster-a:nomatch*::failures";
            try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), equalTo(0));
            }
        }

        // an error is thrown if there is a concrete index that does not match
        {
            String q = "FROM nomatch*::failures,cluster-a:nomatch::failures";
            String expectedError = "Unknown index [cluster-a:nomatch::failures]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }

        // an error is thrown if there are no matching indices at all - local with wildcard, remote with wildcard
        {
            String q = "FROM nomatch*::failures,cluster-a:nomatch*::failures";
            try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                assertThat(getValuesList(resp).size(), equalTo(0));
            }
        }
        {
            String q = "FROM nomatch::failures,cluster-a:nomatch::failures";
            String expectedError = "Unknown index [cluster-a:nomatch::failures]; Unknown index [nomatch::failures]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }
        {
            String q = "FROM nomatch::failures,cluster-a:nomatch*::failures";
            String expectedError = "Unknown index [nomatch::failures]";
            expectVerificationExceptionForQuery(q, expectedError, requestIncludeMeta);
        }

        // --- test against 3 clusters

        // missing concrete index (on remote) is error
        {
            String localIndexName = randomFrom(localIndex, IDX_ALIAS, FILTERED_IDX_ALIAS);
            String remote2IndexName = randomFrom(remote2Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
            String q = Strings.format(
                "FROM %s*::failures,cluster-a:nomatch::failures,%s:%s*::failures",
                localIndexName,
                REMOTE_CLUSTER_2,
                remote2IndexName
            );
            String expectedError = "Unknown index [cluster-a:nomatch::failures]";
            try {
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
                                localIndexName + "*::failures",
                                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                                localNumShards
                            ),
                            new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch::failures", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0),
                            new ExpectedCluster(
                                REMOTE_CLUSTER_2,
                                remote2IndexName + "*::failures",
                                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                                remote2NumShards
                            )
                        )
                    );

                }
            } finally {
                resetSkipUnavailableDefaults();
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
            resetSkipUnavailableDefaults();
        }
    }

    public void testFailureStoreSearchesWhereNonExistentClusterIsSpecifiedWithWildcards() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // a query which matches no remote cluster is not a cross cluster search
        try (
            EsqlQueryResponse resp = runQuery(
                "from logs-*::failures,x*:no_such_index*::failures | stats count (error.type)",
                requestIncludeMeta
            )
        ) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(5L)));

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
                "FROM logs-*::failures,no_such_index*::failures,cluster-a:no_such_index*::failures,cluster-foo*:*::failures "
                    + "| STATS count (error.type)",
                requestIncludeMeta
            )
        ) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            List<List<Object>> values = getValuesList(resp);
            assertThat(values, hasSize(1));
            assertThat(values.get(0), equalTo(List.of(5L)));

            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.isPartial(), equalTo(false));

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("no_such_index*::failures"));
            assertClusterInfoSuccess(remoteCluster, 0);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertThat(localCluster.getIndexExpression(), equalTo("logs-*::failures,no_such_index*::failures"));
            assertClusterInfoSuccess(localCluster, localNumShards);
        } finally {
            resetSkipUnavailableDefaults();
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

    /**
     * Searches with LIMIT 0 are used by Kibana to get a list of columns. After the initial planning
     * (which involves cross-cluster field-caps calls), it is a coordinator only operation at query time
     * which uses a different pathway compared to queries that require data node (and remote data node) operations
     * at query time.
     *
     * Note: the tests covering "nonmatching indices" also do LIMIT 0 tests.
     * This one is mostly focuses on took time values.
     */
    public void testCCSExecutionOnFailureStoreSearchesWithLimit0() throws Exception {
        setupTwoClusters();
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        // Ensure non-cross cluster queries have overall took time
        try (EsqlQueryResponse resp = runQuery("FROM logs*::failures | LIMIT 0", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(false));
            assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        }

        // ensure cross-cluster searches have overall took time and correct per-cluster details in EsqlExecutionInfo
        try (EsqlQueryResponse resp = runQuery("FROM logs-*::failures,cluster-a:*::failures | LIMIT 0", requestIncludeMeta)) {
            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            assertThat(executionInfo.isPartial(), equalTo(false));
            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("*::failures"));
            assertThat(remoteCluster.getTook().millis(), lessThanOrEqualTo(overallTookMillis));
            assertClusterInfoSuccess(remoteCluster, 0);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, "logs-*::failures", 0, overallTookMillis);
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
            assertThat(values.get(0), equalTo(List.of(45L, testClusterInfo.get("local.index.ds"))));
            assertThat(values.get(1), equalTo(List.of(285L, REMOTE_CLUSTER_1 + ":" + testClusterInfo.get("remote1.index.ds"))));

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

    public void testFailureStoreMetadataIndex() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        try (
            EsqlQueryResponse resp = runQuery(
                Strings.format(
                    "FROM logs-*::failures,%s:logs-*::failures METADATA _index | stats count(error.type) by _index | sort _index",
                    REMOTE_CLUSTER_1
                ),
                requestIncludeMeta
            )
        ) {
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0), equalTo(List.of(5L, testClusterInfo.get("local.index.fs"))));
            assertThat(values.get(1), equalTo(List.of(5L, REMOTE_CLUSTER_1 + ":" + testClusterInfo.get("remote1.index.fs"))));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));
            assertThat(executionInfo.isPartial(), equalTo(false));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterInfoSuccess(remoteCluster, "logs-*::failures", remoteNumShards, overallTookMillis);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, "logs-*::failures", localNumShards, overallTookMillis);
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

    public void testProfileFailureStore() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        assumeTrue("pragmas only enabled on snapshot builds", Build.current().isSnapshot());
        // uses shard partitioning as segments can be merged during these queries
        var pragmas = new QueryPragmas(Settings.builder().put(QueryPragmas.DATA_PARTITIONING.getKey(), DataPartitioning.SHARD).build());
        // Use single replicas for the target indices, to make sure we hit the same set of target nodes
        client(LOCAL_CLUSTER).admin()
            .indices()
            .prepareUpdateSettings("logs-1::failures")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put("index.routing.rebalance.enable", "none"))
            .get();
        waitForNoInitializingShards(client(LOCAL_CLUSTER), TimeValue.timeValueSeconds(30), "logs-1");
        client(REMOTE_CLUSTER_1).admin()
            .indices()
            .prepareUpdateSettings("logs-2::failures")
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put("index.routing.rebalance.enable", "none"))
            .get();
        waitForNoInitializingShards(client(REMOTE_CLUSTER_1), TimeValue.timeValueSeconds(30), "logs-2");
        final int localOnlyProfiles;
        {
            try (
                EsqlQueryResponse resp = runQuery(
                    syncEsqlQueryRequest("FROM logs*::failures | stats count(error.type)").pragmas(pragmas).profile(true)
                )
            ) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(5L)));
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
            try (
                EsqlQueryResponse resp = runQuery(
                    syncEsqlQueryRequest("FROM c*:logs-*::failures | stats count(error.type)").pragmas(pragmas).profile(true)
                )
            ) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(5L)));
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
                assertClusterInfoSuccess(remoteCluster, "logs-*::failures", remoteNumShards, executionInfo.overallTook().millis());

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertNull(localCluster);
            }
        }
        final int allProfiles;
        {
            EsqlQueryRequest request = syncEsqlQueryRequest("FROM logs-*::failures,c*:logs-*::failures | stats total = count(error.type)")
                .pragmas(pragmas)
                .profile(true);
            try (EsqlQueryResponse resp = runQuery(request)) {
                List<List<Object>> values = getValuesList(resp);
                assertThat(values.get(0), equalTo(List.of(10L)));
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
                assertClusterInfoSuccess(remoteCluster, "logs-*::failures", remoteNumShards, overallTookMillis);

                EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
                assertClusterInfoSuccess(localCluster, "logs-*::failures", localNumShards, overallTookMillis);
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

    public void testFailureStoreWarnings() throws Exception {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote1.num_shards");

        EsqlQueryRequest request = syncEsqlQueryRequest(
            "FROM logs-*::failures,c*:logs-*::failures | EVAL ip = to_ip(error.type) | STATS total = count(error.type) by ip | LIMIT 10"
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
            assertThat(warnings.size(), greaterThanOrEqualTo(1));
            List<List<Object>> values = getValuesList(resp);
            assertThat(values.get(0).get(0), equalTo(10L));
            assertNull(values.get(0).get(1));

            EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
            assertNotNull(executionInfo);
            assertThat(executionInfo.isCrossClusterSearch(), is(true));
            assertThat(executionInfo.includeCCSMetadata(), is(false));
            long overallTookMillis = executionInfo.overallTook().millis();
            assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertClusterInfoSuccess(remoteCluster, "logs-*::failures", remoteNumShards, overallTookMillis);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER);
            assertClusterInfoSuccess(localCluster, "logs-*::failures", localNumShards, overallTookMillis);

            latch.countDown();
        }, e -> {
            latch.countDown();
            throw new AssertionError(e);
        }));
        assertTrue(latch.await(30, TimeUnit.SECONDS));
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

    @Override
    protected Map<String, Object> setupClusters(int numClusters) throws IOException {
        assert numClusters == 2 || numClusters == 3 : "2 or 3 clusters supported not: " + numClusters;
        int numShardsLocal = randomIntBetween(1, 5);
        Map<String, Object> localDs = populateDataStream(LOCAL_CLUSTER, LOCAL_INDEX, numShardsLocal, 10, 5);

        int numShardsRemote = randomIntBetween(1, 5);
        Map<String, Object> remoteDs = populateDataStream(REMOTE_CLUSTER_1, REMOTE_INDEX, numShardsRemote, 10, 5);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", LOCAL_INDEX);
        clusterInfo.put("local.index.ds", localDs.get("index.ds"));
        clusterInfo.put("local.index.fs", localDs.get("index.fs"));
        clusterInfo.put("remote1.num_shards", numShardsRemote);
        clusterInfo.put("remote1.index", REMOTE_INDEX);
        clusterInfo.put("remote1.index.ds", remoteDs.get("index.ds"));
        clusterInfo.put("remote1.index.fs", remoteDs.get("index.fs"));
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", REMOTE_INDEX);

        if (numClusters == 3) {
            int numShardsRemote2 = randomIntBetween(1, 5);
            Map<String, Object> remoteDs2 = populateDataStream(REMOTE_CLUSTER_2, REMOTE_INDEX, numShardsRemote2, 10, 5);
            clusterInfo.put("remote2.index", REMOTE_INDEX);
            clusterInfo.put("remote2.index.ds", remoteDs2.get("index.ds"));
            clusterInfo.put("remote2.index.fs", remoteDs2.get("index.fs"));
            clusterInfo.put("remote2.num_shards", numShardsRemote2);
        }

        String skipUnavailableKey = Strings.format("cluster.remote.%s.skip_unavailable", REMOTE_CLUSTER_1);
        Setting<?> skipUnavailableSetting = cluster(REMOTE_CLUSTER_1).clusterService().getClusterSettings().get(skipUnavailableKey);
        boolean skipUnavailable = (boolean) cluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY).clusterService()
            .getClusterSettings()
            .get(skipUnavailableSetting);
        clusterInfo.put("remote.skip_unavailable", skipUnavailable);

        return clusterInfo;
    }

    protected Map<String, Object> populateDataStream(String clusterAlias, String indexName, int numShards, int numDocs, int numFailures)
        throws IOException {
        Client client = client(clusterAlias);
        var template = ComposableIndexTemplate.builder()
            .indexPatterns(List.of(indexName))
            .template(
                Template.builder()
                    .settings(Settings.builder().put("index.number_of_shards", numShards))
                    .mappings(CompressedXContent.fromJSON("""
                        {
                            "properties": {
                              "@timestamp" : {
                                "type": "date"
                              },
                              "id": {
                                "type": "keyword"
                              },
                              "tag": {
                                "type": "keyword"
                              },
                              "v": {
                                "type": "long"
                              },
                              "const": {
                                "type": "long"
                              }
                            }
                        }"""))
                    .dataStreamOptions(new DataStreamOptions.Template(new DataStreamFailureStore.Template(true, null)))
                    .build()
            )
            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
            .build();
        var request = new TransportPutComposableIndexTemplateAction.Request(indexName + "-template");
        request.indexTemplate(template);
        assertAcked(client.execute(TransportPutComposableIndexTemplateAction.TYPE, request));

        Long ts = System.currentTimeMillis();
        Set<String> ids = new HashSet<>();
        String tag = Strings.isEmpty(clusterAlias) ? "local" : "remote";
        String idPrefix = Strings.isEmpty(clusterAlias) ? "" : clusterAlias + "-";
        for (int i = 0; i < numDocs; i++) {
            String id = Long.toString(NEXT_DOC_ID.incrementAndGet());
            client.prepareIndex(indexName)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setSource("id", idPrefix + id, "@timestamp", ts, "tag", tag, "v", Strings.isEmpty(clusterAlias) ? i : i * i)
                .get();
            ids.add(id);
        }
        Set<String> failIds = new HashSet<>();
        for (int i = 0; i < numFailures; i++) {
            String id = Long.toString(NEXT_DOC_ID.incrementAndGet());
            client.prepareIndex(indexName)
                .setOpType(DocWriteRequest.OpType.CREATE)
                .setSource("id", idPrefix + id, "@timestamp", ts, "tag", tag, "v", "invalid")
                .get();
            failIds.add(id);
        }
        DataStream createdDataStream = client.execute(
            GetDataStreamAction.INSTANCE,
            new GetDataStreamAction.Request(TimeValue.THIRTY_SECONDS, new String[] { indexName })
        ).actionGet().getDataStreams().getFirst().getDataStream();
        String writeIndex = createdDataStream.getIndices().getFirst().getName();
        String failureIndex = createdDataStream.getFailureIndices().getFirst().getName();
        client.admin().indices().prepareRefresh(writeIndex).get();
        client.admin().indices().prepareRefresh(failureIndex).get();
        return Map.of("index.ds", writeIndex, "docs.ds", ids, "index.fs", failureIndex, "docs.fs", failIds);
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
    void createDataStreamAliases(int numClusters) {
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
}
