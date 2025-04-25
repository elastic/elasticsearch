/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase.randomIncludeCCSMetadata;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * This IT test is the dual of CrossClustersEnrichIT, which tests "happy path"
 * and this one tests unavailable cluster scenarios using (most of) the same tests.
 */
public class CrossClusterEnrichUnavailableClustersIT extends AbstractEnrichBasedCrossClusterTestCase {

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    @Override
    protected boolean tolerateErrorsWhenWipingEnrichPolicies() {
        // attempt to wipe will fail since some clusters are already closed
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
        plugins.add(EsqlPluginWithEnterpriseOrTrialLicense.class);
        return plugins;
    }

    public void testEnrichWithHostsPolicyAndDisconnectedRemotesWithSkipUnavailableTrue() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            {
                Enrich.Mode mode = randomFrom(Enrich.Mode.values());
                String query = "FROM *:events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    List<List<Object>> rows = getValuesList(resp);
                    assertThat(rows.size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertCCSExecutionInfoDetails(executionInfo);

                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(cluster2.getTotalShards(), greaterThanOrEqualTo(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(cluster2.getSuccessfulShards()));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));
                }
            }

            // close remote-cluster-2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();

            {
                Enrich.Mode mode = randomFrom(Enrich.Mode.values());
                String query = "FROM *:events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    List<ColumnInfoImpl> columns = resp.columns();
                    assertThat(columns.size(), equalTo(1));
                    // column from an empty result should be {"name":"<no-fields>","type":"null"}
                    assertThat(columns.get(0).name(), equalTo("<no-fields>"));
                    assertThat(columns.get(0).type(), equalTo(DataType.NULL));

                    List<List<Object>> rows = getValuesList(resp);
                    assertThat(rows.size(), equalTo(0));

                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertCCSExecutionInfoDetails(executionInfo);

                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster2.getTotalShards(), equalTo(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testEnrichWithHostsPolicyAndDisconnectedRemotesWithSkipUnavailableFalse() throws IOException {
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, false);

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
            Boolean requestIncludeMeta = includeCCSMetadata.v1();
            boolean responseExpectMeta = includeCCSMetadata.v2();

            {
                Enrich.Mode mode = randomFrom(Enrich.Mode.values());
                String query = "FROM *:events | EVAL ip= TO_STR(host) | " + enrichHosts(mode) + " | STATS c = COUNT(*) by os | SORT os";
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    List<List<Object>> rows = getValuesList(resp);
                    assertThat(rows.size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertCCSExecutionInfoDetails(executionInfo);

                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(cluster2.getTotalShards(), greaterThanOrEqualTo(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(cluster2.getSuccessfulShards()));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));
                }
            }

            // close remote-cluster-2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();
            {
                Enrich.Mode mode = randomFrom(Enrich.Mode.values());
                String query = "FROM *:events | eval ip= TO_STR(host) | " + enrichHosts(mode) + " | stats c = COUNT(*) by os | SORT os";
                Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testEnrichTwiceThenAggsWithUnavailableRemotes() throws IOException {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        boolean skipUnavailableRemote1 = randomBoolean();
        setSkipUnavailable(REMOTE_CLUSTER_1, skipUnavailableRemote1);
        setSkipUnavailable(REMOTE_CLUSTER_2, true);

        try {
            // close remote-cluster-2 so that it is unavailable
            cluster(REMOTE_CLUSTER_2).close();

            for (var hostMode : Enrich.Mode.values()) {
                String query = String.format(Locale.ROOT, """
                    FROM *:events,events
                    | eval ip= TO_STR(host)
                    | %s
                    | %s
                    | stats c = COUNT(*) by vendor
                    | sort vendor
                    """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.COORDINATOR));
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));
                    assertCCSExecutionInfoDetails(executionInfo);

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(cluster1.getTotalShards(), greaterThanOrEqualTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(cluster1.getSuccessfulShards()));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster2.getTotalShards(), equalTo(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(localCluster.getTotalShards(), greaterThan(0));
                    assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));
                    assertThat(localCluster.getSkippedShards(), equalTo(0));
                    assertThat(localCluster.getFailedShards(), equalTo(0));
                }
            }

            // close remote-cluster-1 so that it is also unavailable
            cluster(REMOTE_CLUSTER_1).close();

            for (var hostMode : Enrich.Mode.values()) {
                String query = String.format(Locale.ROOT, """
                    FROM *:events,events
                    | eval ip= TO_STR(host)
                    | %s
                    | %s
                    | stats c = COUNT(*) by vendor
                    | sort vendor
                    """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.COORDINATOR));
                if (skipUnavailableRemote1 == false) {
                    Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                    assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
                } else {
                    try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                        assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                        EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                        assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                        assertThat(executionInfo.clusterAliases(), equalTo(Set.of("", REMOTE_CLUSTER_1, REMOTE_CLUSTER_2)));
                        assertCCSExecutionInfoDetails(executionInfo);

                        EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                        assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                        assertThat(cluster1.getTotalShards(), equalTo(0));
                        assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                        assertThat(cluster1.getSkippedShards(), equalTo(0));
                        assertThat(cluster1.getFailedShards(), equalTo(0));
                        assertThat(cluster1.getTook().millis(), greaterThanOrEqualTo(0L));

                        EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                        assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                        assertThat(cluster2.getTotalShards(), equalTo(0));
                        assertThat(cluster2.getSuccessfulShards(), equalTo(0));
                        assertThat(cluster2.getSkippedShards(), equalTo(0));
                        assertThat(cluster2.getFailedShards(), equalTo(0));

                        EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                        assertThat(localCluster.getTotalShards(), greaterThan(0));
                        assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));
                        assertThat(localCluster.getSkippedShards(), equalTo(0));
                        assertThat(localCluster.getFailedShards(), equalTo(0));
                    }
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testEnrichCoordinatorThenAnyWithSingleUnavailableRemoteAndLocal() throws IOException {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        boolean skipUnavailableRemote1 = randomBoolean();
        setSkipUnavailable(REMOTE_CLUSTER_1, skipUnavailableRemote1);

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();
            String query = String.format(Locale.ROOT, """
                FROM %s:events,events
                | eval ip= TO_STR(host)
                | %s
                | %s
                | stats c = COUNT(*) by vendor
                | sort vendor
                """, REMOTE_CLUSTER_1, enrichHosts(Enrich.Mode.COORDINATOR), enrichVendors(Enrich.Mode.ANY));
            if (skipUnavailableRemote1 == false) {
                Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
            } else {
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThanOrEqualTo(1));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(
                        executionInfo.clusterAliases(),
                        equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1))
                    );
                    assertCCSExecutionInfoDetails(executionInfo);

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(localCluster.getTotalShards(), greaterThan(0));
                    assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));
                    assertThat(localCluster.getSkippedShards(), equalTo(0));
                    assertThat(localCluster.getFailedShards(), equalTo(0));
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testEnrichCoordinatorThenAnyWithSingleUnavailableRemoteAndNotLocal() throws IOException {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        boolean skipUnavailableRemote1 = randomBoolean();
        setSkipUnavailable(REMOTE_CLUSTER_1, skipUnavailableRemote1);

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();
            String query = String.format(Locale.ROOT, """
                FROM %s:events
                | eval ip= TO_STR(host)
                | %s
                | %s
                | stats c = COUNT(*) by vendor
                | sort vendor
                """, REMOTE_CLUSTER_1, enrichHosts(Enrich.Mode.COORDINATOR), enrichVendors(Enrich.Mode.ANY));
            if (skipUnavailableRemote1 == false) {
                Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
            } else {
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    List<ColumnInfoImpl> columns = resp.columns();
                    assertThat(columns.size(), equalTo(1));
                    // column from an empty result should be {"name":"<no-fields>","type":"null"}
                    assertThat(columns.get(0).name(), equalTo("<no-fields>"));
                    assertThat(columns.get(0).type(), equalTo(DataType.NULL));

                    assertThat(getValuesList(resp).size(), equalTo(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(executionInfo.clusterAliases(), equalTo(Set.of(REMOTE_CLUSTER_1)));
                    assertCCSExecutionInfoDetails(executionInfo);

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    public void testEnrichRemoteWithVendor() throws IOException {
        Tuple<Boolean, Boolean> includeCCSMetadata = randomIncludeCCSMetadata();
        Boolean requestIncludeMeta = includeCCSMetadata.v1();
        boolean responseExpectMeta = includeCCSMetadata.v2();

        boolean skipUnavailableRemote2 = randomBoolean();
        setSkipUnavailable(REMOTE_CLUSTER_1, true);
        setSkipUnavailable(REMOTE_CLUSTER_2, skipUnavailableRemote2);

        try {
            // close remote-cluster-1 so that it is unavailable
            cluster(REMOTE_CLUSTER_1).close();

            for (Enrich.Mode hostMode : List.of(Enrich.Mode.ANY, Enrich.Mode.REMOTE)) {
                var query = String.format(Locale.ROOT, """
                    FROM *:events,events
                    | eval ip= TO_STR(host)
                    | %s
                    | %s
                    | stats c = COUNT(*) by vendor
                    | sort vendor
                    """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.REMOTE));
                try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                    assertThat(getValuesList(resp).size(), greaterThan(0));
                    EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                    assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                    assertThat(
                        executionInfo.clusterAliases(),
                        equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2))
                    );
                    assertCCSExecutionInfoDetails(executionInfo);

                    EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                    assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                    assertThat(cluster1.getTotalShards(), equalTo(0));
                    assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                    assertThat(cluster1.getSkippedShards(), equalTo(0));
                    assertThat(cluster1.getFailedShards(), equalTo(0));
                    assertThat(cluster1.getTook().millis(), greaterThanOrEqualTo(0L));

                    EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                    assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(cluster2.getTotalShards(), greaterThan(0));
                    assertThat(cluster2.getSuccessfulShards(), equalTo(cluster2.getSuccessfulShards()));
                    assertThat(cluster2.getSkippedShards(), equalTo(0));
                    assertThat(cluster2.getFailedShards(), equalTo(0));

                    EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                    assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                    assertThat(localCluster.getTotalShards(), greaterThan(0));
                    assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));
                    assertThat(localCluster.getSkippedShards(), equalTo(0));
                    assertThat(localCluster.getFailedShards(), equalTo(0));
                }
            }

            // close remote-cluster-2 so that it is also unavailable
            cluster(REMOTE_CLUSTER_2).close();

            for (Enrich.Mode hostMode : List.of(Enrich.Mode.ANY, Enrich.Mode.REMOTE)) {
                var query = String.format(Locale.ROOT, """
                    FROM *:events,events
                    | eval ip= TO_STR(host)
                    | %s
                    | %s
                    | stats c = COUNT(*) by vendor
                    | sort vendor
                    """, enrichHosts(hostMode), enrichVendors(Enrich.Mode.REMOTE));
                if (skipUnavailableRemote2 == false) {
                    Exception exception = expectThrows(Exception.class, () -> runQuery(query, requestIncludeMeta));
                    assertTrue(ExceptionsHelper.isRemoteUnavailableException(exception));
                } else {

                    try (EsqlQueryResponse resp = runQuery(query, requestIncludeMeta)) {
                        assertThat(getValuesList(resp).size(), greaterThan(0));
                        EsqlExecutionInfo executionInfo = resp.getExecutionInfo();
                        assertThat(executionInfo.includeCCSMetadata(), equalTo(responseExpectMeta));
                        assertThat(
                            executionInfo.clusterAliases(),
                            equalTo(Set.of(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY, REMOTE_CLUSTER_1, REMOTE_CLUSTER_2))
                        );
                        assertCCSExecutionInfoDetails(executionInfo);

                        EsqlExecutionInfo.Cluster cluster1 = executionInfo.getCluster(REMOTE_CLUSTER_1);
                        assertThat(cluster1.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                        assertThat(cluster1.getTotalShards(), equalTo(0));
                        assertThat(cluster1.getSuccessfulShards(), equalTo(0));
                        assertThat(cluster1.getSkippedShards(), equalTo(0));
                        assertThat(cluster1.getFailedShards(), equalTo(0));
                        assertThat(cluster1.getTook().millis(), greaterThanOrEqualTo(0L));

                        EsqlExecutionInfo.Cluster cluster2 = executionInfo.getCluster(REMOTE_CLUSTER_2);
                        assertThat(cluster2.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
                        assertThat(cluster2.getTotalShards(), equalTo(0));
                        assertThat(cluster2.getSuccessfulShards(), equalTo(0));
                        assertThat(cluster2.getSkippedShards(), equalTo(0));
                        assertThat(cluster2.getFailedShards(), equalTo(0));

                        EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
                        assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
                        assertThat(localCluster.getTotalShards(), greaterThan(0));
                        assertThat(localCluster.getSuccessfulShards(), equalTo(localCluster.getTotalShards()));
                        assertThat(localCluster.getSkippedShards(), equalTo(0));
                        assertThat(localCluster.getFailedShards(), equalTo(0));
                    }
                }
            }
        } finally {
            clearSkipUnavailable();
        }
    }

    private static void assertCCSExecutionInfoDetails(EsqlExecutionInfo executionInfo) {
        assertThat(executionInfo.overallTook().millis(), greaterThanOrEqualTo(0L));
        assertTrue(executionInfo.isCrossClusterSearch());

        boolean hasPartials = false;
        for (String clusterAlias : executionInfo.clusterAliases()) {
            EsqlExecutionInfo.Cluster cluster = executionInfo.getCluster(clusterAlias);
            assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(cluster.getTook().millis(), lessThanOrEqualTo(executionInfo.overallTook().millis()));
            if (cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.PARTIAL
                || cluster.getStatus() == EsqlExecutionInfo.Cluster.Status.SKIPPED) {
                hasPartials = true;
            }
        }
        assertThat(executionInfo.isPartial(), equalTo(hasPartials));
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
