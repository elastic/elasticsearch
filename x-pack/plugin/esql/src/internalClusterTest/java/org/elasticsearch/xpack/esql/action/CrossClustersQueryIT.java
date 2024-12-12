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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.compute.operator.DriverProfile;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractMultiClustersTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class CrossClustersQueryIT extends AbstractMultiClustersTestCase {
    private static final String REMOTE_CLUSTER_1 = "cluster-a";
    private static final String REMOTE_CLUSTER_2 = "remote-b";
    private static String LOCAL_INDEX = "logs-1";
    private static String IDX_ALIAS = "alias1";
    private static String FILTERED_IDX_ALIAS = "alias-filtered-1";
    private static String REMOTE_INDEX = "logs-2";

    @Override
    protected List<String> remoteClusterAlias() {
        return List.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2);
    }

    @Override
    protected Map<String, Boolean> skipUnavailableForRemoteClusters() {
        return Map.of(REMOTE_CLUSTER_1, randomBoolean());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPlugin.class);
        plugins.add(InternalExchangePlugin.class);
        return plugins;
    }

    public static class InternalExchangePlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return List.of(
                Setting.timeSetting(
                    ExchangeService.INACTIVE_SINKS_INTERVAL_SETTING,
                    TimeValue.timeValueSeconds(30),
                    Setting.Property.NodeScope
                )
            );
        }
    }

    public void testSuccessfulPathways() {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

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

    public void testSearchesAgainstNonMatchingIndicesWithLocalOnly() {
        Map<String, Object> testClusterInfo = setupClusters(2);
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
            VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, false));
            assertThat(e.getDetailedMessage(), containsString("Unknown index [nomatch]"));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(VerificationException.class, () -> runQuery(limit0, false));
            assertThat(e.getDetailedMessage(), containsString("Unknown index [nomatch]"));
        }
        {
            String q = "FROM nomatch*";
            VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, false));
            assertThat(e.getDetailedMessage(), containsString("Unknown index [nomatch*]"));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(VerificationException.class, () -> runQuery(limit0, false));
            assertThat(e.getDetailedMessage(), containsString("Unknown index [nomatch*]"));
        }
    }

    public void testSearchesAgainstIndicesWithNoMappingsSkipUnavailableTrue() {
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

    public void testSearchesAgainstNonMatchingIndicesWithSkipUnavailableTrue() {
        int numClusters = 3;
        Map<String, Object> testClusterInfo = setupClusters(numClusters);
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remote1NumShards = (Integer) testClusterInfo.get("remote.num_shards");
        int remote2NumShards = (Integer) testClusterInfo.get("remote2.num_shards");
        String localIndex = (String) testClusterInfo.get("local.index");
        String remote1Index = (String) testClusterInfo.get("remote.index");
        String remote2Index = (String) testClusterInfo.get("remote2.index");

        createIndexAliases(numClusters);
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        try {
            // missing concrete local index is fatal
            {
                String q = "FROM nomatch,cluster-a:" + randomFrom(remote1Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [nomatch]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [nomatch]"));
            }

            // missing concrete remote index is not fatal when skip_unavailable=true (as long as an index matches on another cluster)
            {
                String localIndexName = randomFrom(localIndex, IDX_ALIAS, FILTERED_IDX_ALIAS);
                String q = Strings.format("FROM %s,cluster-a:nomatch", localIndexName);
                try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(
                            new ExpectedCluster(LOCAL_CLUSTER, localIndexName, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, localNumShards),
                            new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0)
                        )
                    );
                }

                String limit0 = q + " | LIMIT 0";
                try (EsqlQueryResponse resp = runQuery(limit0, requestIncludeMeta)) {
                    assertThat(resp.columns().size(), greaterThan(0));
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(
                            new ExpectedCluster(LOCAL_CLUSTER, localIndexName, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                            new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0)
                        )
                    );
                }
            }

            // since there is at least one matching index in the query, the missing wildcarded local index is not an error
            {
                String remoteIndexName = randomFrom(remote1Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
                String q = "FROM nomatch*,cluster-a:" + remoteIndexName;
                try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(
                            // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                            new ExpectedCluster(LOCAL_CLUSTER, "nomatch*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                            new ExpectedCluster(
                                REMOTE_CLUSTER_1,
                                remoteIndexName,
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
                            new ExpectedCluster(REMOTE_CLUSTER_1, remoteIndexName, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0)
                        )
                    );
                }
            }

            // since at least one index of the query matches on some cluster, a wildcarded index on skip_un=true is not an error
            {
                String localIndexName = randomFrom(localIndex, IDX_ALIAS, FILTERED_IDX_ALIAS);
                String q = Strings.format("FROM %s,cluster-a:nomatch*", localIndexName);
                try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(
                            new ExpectedCluster(LOCAL_CLUSTER, localIndexName, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, localNumShards),
                            new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch*", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0)
                        )
                    );
                }

                String limit0 = q + " | LIMIT 0";
                try (EsqlQueryResponse resp = runQuery(limit0, requestIncludeMeta)) {
                    assertThat(resp.columns().size(), greaterThan(0));
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(
                            new ExpectedCluster(LOCAL_CLUSTER, localIndexName, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                            new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch*", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0)
                        )
                    );
                }
            }

            // an error is thrown if there are no matching indices at all, even when the cluster is skip_unavailable=true
            {
                // with non-matching concrete index
                String q = "FROM cluster-a:nomatch";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch]"));
            }

            // an error is thrown if there are no matching indices at all, even when the cluster is skip_unavailable=true and the
            // index was wildcarded
            {
                // with non-matching wildcard index
                String q = "FROM cluster-a:nomatch*";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*]"));
            }

            // an error is thrown if there are no matching indices at all - local with wildcard, remote with concrete
            {
                String q = "FROM nomatch*,cluster-a:nomatch";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch,nomatch*]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch,nomatch*]"));
            }

            // an error is thrown if there are no matching indices at all - local with wildcard, remote with wildcard
            {
                String q = "FROM nomatch*,cluster-a:nomatch*";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*,nomatch*]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*,nomatch*]"));
            }

            // an error is thrown if there are no matching indices at all - local with concrete, remote with concrete
            {
                String q = "FROM nomatch,cluster-a:nomatch";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch,nomatch]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch,nomatch]"));
            }

            // an error is thrown if there are no matching indices at all - local with concrete, remote with wildcard
            {
                String q = "FROM nomatch,cluster-a:nomatch*";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*,nomatch]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*,nomatch]"));
            }

            // since cluster-a is skip_unavailable=true and at least one cluster has a matching indices, no error is thrown
            {
                // TODO solve in follow-on PR which does skip_unavailable handling at execution time
                // String q = Strings.format("FROM %s,cluster-a:nomatch,cluster-a:%s*", localIndex, remote1Index);
                // try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                // assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                // EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                // assertThat(executionInfo.isCrossClusterSearch(), is(true));
                // assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                // assertExpectedClustersForMissingIndicesTests(executionInfo, List.of(
                // // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                // new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0),
                // new ExpectedCluster(REMOTE_CLUSTER_1, "*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, remote2NumShards)
                // ));
                // }

                // TODO: handle LIMIT 0 for this case in follow-on PR
                // String limit0 = q + " | LIMIT 0";
                // try (EsqlQueryResponse resp = runQuery(limit0, requestIncludeMeta)) {
                // assertThat(resp.columns().size(), greaterThanOrEqualTo(1));
                // assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(0));
                // EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                // assertThat(executionInfo.isCrossClusterSearch(), is(true));
                // assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                // assertExpectedClustersForMissingIndicesTests(executionInfo, List.of(
                // // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                // new ExpectedCluster(LOCAL_CLUSTER, localIndex, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                // new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch," + remote1Index + "*", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0)
                // ));
                // }
            }

            // tests with three clusters ---

            // since cluster-a is skip_unavailable=true and at least one cluster has a matching indices, no error is thrown
            // cluster-a should be marked as SKIPPED with VerificationException
            {
                String remote2IndexName = randomFrom(remote2Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
                String q = Strings.format("FROM nomatch*,cluster-a:nomatch,%s:%s", REMOTE_CLUSTER_2, remote2IndexName);
                try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(
                            // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                            new ExpectedCluster(LOCAL_CLUSTER, "nomatch*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                            new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0),
                            new ExpectedCluster(
                                REMOTE_CLUSTER_2,
                                remote2IndexName,
                                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                                remote2NumShards
                            )
                        )
                    );
                }

                String limit0 = q + " | LIMIT 0";
                try (EsqlQueryResponse resp = runQuery(limit0, requestIncludeMeta)) {
                    assertThat(resp.columns().size(), greaterThanOrEqualTo(1));
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(
                            // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                            new ExpectedCluster(LOCAL_CLUSTER, "nomatch*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                            new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0),
                            new ExpectedCluster(REMOTE_CLUSTER_2, remote2IndexName, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0)
                        )
                    );
                }
            }

            // since cluster-a is skip_unavailable=true and at least one cluster has a matching indices, no error is thrown
            // cluster-a should be marked as SKIPPED with a "NoMatchingIndicesException" since a wildcard index was requested
            {
                String remote2IndexName = randomFrom(remote2Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
                String q = Strings.format("FROM nomatch*,cluster-a:nomatch*,%s:%s", REMOTE_CLUSTER_2, remote2IndexName);
                try (EsqlQueryResponse resp = runQuery(q, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(
                            // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                            new ExpectedCluster(LOCAL_CLUSTER, "nomatch*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                            new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch*", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0),
                            new ExpectedCluster(
                                REMOTE_CLUSTER_2,
                                remote2IndexName,
                                EsqlExecutionInfo.Cluster.Status.SUCCESSFUL,
                                remote2NumShards
                            )
                        )
                    );
                }

                String limit0 = q + " | LIMIT 0";
                try (EsqlQueryResponse resp = runQuery(limit0, requestIncludeMeta)) {
                    assertThat(resp.columns().size(), greaterThanOrEqualTo(1));
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.isCrossClusterSearch(), is(true));
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertExpectedClustersForMissingIndicesTests(
                        executionInfo,
                        List.of(
                            // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                            new ExpectedCluster(LOCAL_CLUSTER, "nomatch*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                            new ExpectedCluster(REMOTE_CLUSTER_1, "nomatch*", EsqlExecutionInfo.Cluster.Status.SKIPPED, 0),
                            new ExpectedCluster(REMOTE_CLUSTER_2, remote2IndexName, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0)
                        )
                    );
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testSearchesAgainstNonMatchingIndicesWithSkipUnavailableFalse() {
        int numClusters = 3;
        Map<String, Object> testClusterInfo = setupClusters(numClusters);
        int remote1NumShards = (Integer) testClusterInfo.get("remote.num_shards");
        String localIndex = (String) testClusterInfo.get("local.index");
        String remote1Index = (String) testClusterInfo.get("remote.index");
        String remote2Index = (String) testClusterInfo.get("remote2.index");

        createIndexAliases(numClusters);
        setSkipUnavailable(REMOTE_CLUSTER_1, false);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        try {
            // missing concrete local index is an error
            {
                String q = "FROM nomatch,cluster-a:" + remote1Index;
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [nomatch]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [nomatch]"));
            }

            // missing concrete remote index is fatal when skip_unavailable=false
            {
                String q = "FROM logs*,cluster-a:nomatch";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch]"));
            }

            // No error since local non-matching has wildcard and the remote cluster matches
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
                            // local cluster is never marked as SKIPPED even when no matcing indices - just marked as 0 shards searched
                            new ExpectedCluster(LOCAL_CLUSTER, "nomatch*", EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0),
                            // LIMIT 0 searches always have total shards = 0
                            new ExpectedCluster(REMOTE_CLUSTER_1, remote1IndexName, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL, 0)
                        )
                    );
                }
            }

            // query is fatal since cluster-a has skip_unavailable=false and has no matching indices
            {
                String q = Strings.format("FROM %s,cluster-a:nomatch*", randomFrom(localIndex, IDX_ALIAS, FILTERED_IDX_ALIAS));
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*]"));
            }

            // an error is thrown if there are no matching indices at all - single remote cluster with concrete index expression
            {
                String q = "FROM cluster-a:nomatch";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch]"));
            }

            // an error is thrown if there are no matching indices at all - single remote cluster with wildcard index expression
            {
                String q = "FROM cluster-a:nomatch*";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*]"));
            }

            // an error is thrown if there are no matching indices at all - local with wildcard, remote with concrete
            {
                String q = "FROM nomatch*,cluster-a:nomatch";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch,nomatch*]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch,nomatch*]"));
            }

            // an error is thrown if there are no matching indices at all - local with wildcard, remote with wildcard
            {
                String q = "FROM nomatch*,cluster-a:nomatch*";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*,nomatch*]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*,nomatch*]"));
            }

            // an error is thrown if there are no matching indices at all - local with concrete, remote with concrete
            {
                String q = "FROM nomatch,cluster-a:nomatch";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch,nomatch]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch,nomatch]"));
            }

            // an error is thrown if there are no matching indices at all - local with concrete, remote with wildcard
            {
                String q = "FROM nomatch,cluster-a:nomatch*";
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*,nomatch]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*,nomatch]"));
            }

            // Missing concrete index on skip_unavailable=false cluster is a fatal error, even when another index expression
            // against that cluster matches
            {
                String remote2IndexName = randomFrom(remote2Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
                String q = Strings.format("FROM %s,cluster-a:nomatch,cluster-a:%s*", localIndex, remote2IndexName);
                IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("no such index [nomatch]"));

                // TODO: in follow on PR, add support for throwing a VerificationException from this scenario
                // String limit0 = q + " | LIMIT 0";
                // VerificationException e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                // assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*,nomatch]"));
            }

            // --- test against 3 clusters

            // skip_unavailable=false cluster having no matching indices is a fatal error. This error
            // is fatal at plan time, so it throws VerificationException, not IndexNotFoundException (thrown at execution time)
            {
                String localIndexName = randomFrom(localIndex, IDX_ALIAS, FILTERED_IDX_ALIAS);
                String remote2IndexName = randomFrom(remote2Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
                String q = Strings.format("FROM %s*,cluster-a:nomatch,%s:%s*", localIndexName, REMOTE_CLUSTER_2, remote2IndexName);
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch]"));
            }

            // skip_unavailable=false cluster having no matching indices is a fatal error (even if wildcarded)
            {
                String localIndexName = randomFrom(localIndex, IDX_ALIAS, FILTERED_IDX_ALIAS);
                String remote2IndexName = randomFrom(remote2Index, IDX_ALIAS, FILTERED_IDX_ALIAS);
                String q = Strings.format("FROM %s*,cluster-a:nomatch*,%s:%s*", localIndexName, REMOTE_CLUSTER_2, remote2IndexName);
                VerificationException e = expectThrows(VerificationException.class, () -> runQuery(q, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*]"));

                String limit0 = q + " | LIMIT 0";
                e = expectThrows(VerificationException.class, () -> runQuery(limit0, requestIncludeMeta));
                assertThat(e.getDetailedMessage(), containsString("Unknown index [cluster-a:nomatch*]"));
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    record ExpectedCluster(String clusterAlias, String indexExpression, EsqlExecutionInfo.Cluster.Status status, Integer totalShards) {}

    public void assertExpectedClustersForMissingIndicesTests(EsqlExecutionInfo executionInfo, List<ExpectedCluster> expected) {
        long overallTookMillis = executionInfo.overallTook().millis();
        assertThat(overallTookMillis, greaterThanOrEqualTo(0L));

        Set<String> expectedClusterAliases = expected.stream().map(c -> c.clusterAlias()).collect(Collectors.toSet());
        assertThat(executionInfo.clusterAliases(), equalTo(expectedClusterAliases));

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
                String expectedMsg = "Unknown index [" + expectedCluster.indexExpression() + "]";
                assertThat(msg, cluster.getFailures().get(0).getCause().getMessage(), containsString(expectedMsg));
            }
            // currently failed shards is always zero - change this once we start allowing partial data for individual shard failures
            assertThat(msg, cluster.getFailedShards(), equalTo(0));
        }
    }

    public void testSearchesWhereNonExistentClusterIsSpecifiedWithWildcards() {
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
                "from logs-*,no_such_index*,cluster-a:no_such_index*,cluster-foo*:* | stats sum (v)",
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

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, LOCAL_CLUSTER)));

            EsqlExecutionInfo.Cluster remoteCluster = executionInfo.getCluster(REMOTE_CLUSTER_1);
            assertThat(remoteCluster.getIndexExpression(), equalTo("no_such_index*"));
            assertThat(remoteCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
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
    public void testCCSExecutionOnSearchesWithLimit0() {
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

    public void testMetadataIndex() {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        try (
            EsqlQueryResponse resp = runQuery(
                "FROM logs*,*:logs* METADATA _index | stats sum(v) by _index | sort _index",
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

    public void testProfile() {
        Map<String, Object> testClusterInfo = setupTwoClusters();
        int localNumShards = (Integer) testClusterInfo.get("local.num_shards");
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

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
            request.query("FROM *:logs* | stats sum(v)");
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
            request.query("FROM logs*,*:logs* | stats total = sum(v)");
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
        int remoteNumShards = (Integer) testClusterInfo.get("remote.num_shards");

        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query("FROM logs*,*:logs* | EVAL ip = to_ip(id) | STATS total = sum(v) by ip | LIMIT 10");
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

    protected EsqlQueryResponse runQuery(String query, Boolean ccsMetadataInResponse) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(AbstractEsqlIntegTestCase.randomPragmas());
        request.profile(randomInt(5) == 2);
        request.columnar(randomBoolean());
        if (ccsMetadataInResponse != null) {
            request.includeCCSMetadata(ccsMetadataInResponse);
        }
        return runQuery(request);
    }

    protected EsqlQueryResponse runQuery(EsqlQueryRequest request) {
        return client(LOCAL_CLUSTER).execute(EsqlQueryAction.INSTANCE, request).actionGet(30, TimeUnit.SECONDS);
    }

    /**
     * v1: value to send to runQuery (can be null; null means use default value)
     * v2: whether to expect CCS Metadata in the response (cannot be null)
     * @return
     */
    public static Tuple<Boolean, Boolean> randomIncludeCCSMetadata() {
        return switch (randomIntBetween(1, 3)) {
            case 1 -> new Tuple<>(Boolean.TRUE, Boolean.TRUE);
            case 2 -> new Tuple<>(Boolean.FALSE, Boolean.FALSE);
            case 3 -> new Tuple<>(null, Boolean.FALSE);
            default -> throw new AssertionError("should not get here");
        };
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

    Map<String, Object> setupTwoClusters() {
        return setupClusters(2);
    }

    Map<String, Object> setupClusters(int numClusters) {
        assert numClusters == 2 || numClusters == 3 : "2 or 3 clusters supported not: " + numClusters;
        int numShardsLocal = randomIntBetween(1, 5);
        populateLocalIndices(LOCAL_INDEX, numShardsLocal);

        int numShardsRemote = randomIntBetween(1, 5);
        populateRemoteIndices(REMOTE_CLUSTER_1, REMOTE_INDEX, numShardsRemote);

        Map<String, Object> clusterInfo = new HashMap<>();
        clusterInfo.put("local.num_shards", numShardsLocal);
        clusterInfo.put("local.index", LOCAL_INDEX);
        clusterInfo.put("remote.num_shards", numShardsRemote);
        clusterInfo.put("remote.index", REMOTE_INDEX);

        if (numClusters == 3) {
            int numShardsRemote2 = randomIntBetween(1, 5);
            populateRemoteIndices(REMOTE_CLUSTER_2, REMOTE_INDEX, numShardsRemote2);
            clusterInfo.put("remote2.index", REMOTE_INDEX);
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
                .prepareAliases()
                .addAlias(LOCAL_INDEX, IDX_ALIAS)
                .addAlias(LOCAL_INDEX, FILTERED_IDX_ALIAS, filterBuilder)
                .get();
            assertFalse(indicesAliasesResponse.hasErrors());
        }
        {
            Client remoteClient = client(REMOTE_CLUSTER_1);
            IndicesAliasesResponse indicesAliasesResponse = remoteClient.admin()
                .indices()
                .prepareAliases()
                .addAlias(REMOTE_INDEX, IDX_ALIAS)
                .addAlias(REMOTE_INDEX, FILTERED_IDX_ALIAS, filterBuilder)
                .get();
            assertFalse(indicesAliasesResponse.hasErrors());
        }
        if (numClusters == 3) {
            Client remoteClient = client(REMOTE_CLUSTER_2);
            IndicesAliasesResponse indicesAliasesResponse = remoteClient.admin()
                .indices()
                .prepareAliases()
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

    void populateLocalIndices(String indexName, int numShards) {
        Client localClient = client(LOCAL_CLUSTER);
        assertAcked(
            localClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long")
        );
        for (int i = 0; i < 10; i++) {
            localClient.prepareIndex(indexName).setSource("id", "local-" + i, "tag", "local", "v", i).get();
        }
        localClient.admin().indices().prepareRefresh(indexName).get();
    }

    void populateRemoteIndices(String clusterAlias, String indexName, int numShards) {
        Client remoteClient = client(clusterAlias);
        assertAcked(
            remoteClient.admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(Settings.builder().put("index.number_of_shards", numShards))
                .setMapping("id", "type=keyword", "tag", "type=keyword", "v", "type=long")
        );
        for (int i = 0; i < 10; i++) {
            remoteClient.prepareIndex(indexName).setSource("id", "remote-" + i, "tag", "remote", "v", i * i).get();
        }
        remoteClient.admin().indices().prepareRefresh(indexName).get();
    }

    private void setSkipUnavailable(String clusterAlias, boolean skip) {
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().put("cluster.remote." + clusterAlias + ".skip_unavailable", skip).build())
            .get();
    }

    private void clearSkipUnavailable() {
        Settings.Builder settingsBuilder = Settings.builder()
            .putNull("cluster.remote." + REMOTE_CLUSTER_1 + ".skip_unavailable")
            .putNull("cluster.remote." + REMOTE_CLUSTER_2 + ".skip_unavailable");
        client(LOCAL_CLUSTER).admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(settingsBuilder.build())
            .get();
    }
}
