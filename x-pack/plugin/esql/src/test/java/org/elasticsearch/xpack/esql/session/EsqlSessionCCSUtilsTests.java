/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesFailure;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.indices.IndicesExpressionGrouper;
import org.elasticsearch.license.License;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.internal.XPackLicenseStatus;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.analysis.TableInfo;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.TableIdentifier;
import org.elasticsearch.xpack.esql.type.EsFieldTests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.elasticsearch.xpack.esql.session.EsqlSessionCCSUtils.checkForCcsLicense;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class EsqlSessionCCSUtilsTests extends ESTestCase {

    public void testCreateIndexExpressionFromAvailableClusters() {
        final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        final String remote1Alias = "remote1";
        final String remote2Alias = "remote2";

        // no clusters marked as skipped
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", true));

            String indexExpr = EsqlSessionCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo);
            List<String> list = Arrays.stream(Strings.splitStringByCommaToArray(indexExpr)).toList();
            assertThat(list.size(), equalTo(5));
            assertThat(
                new HashSet<>(list),
                equalTo(Strings.commaDelimitedListToSet("logs*,remote1:*,remote2:mylogs1,remote2:mylogs2,remote2:logs*"))
            );
        }

        // one cluster marked as skipped, so not present in revised index expression
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*,foo", true));
            executionInfo.swapCluster(
                remote2Alias,
                (k, v) -> new EsqlExecutionInfo.Cluster(
                    remote2Alias,
                    "mylogs1,mylogs2,logs*",
                    true,
                    EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );

            String indexExpr = EsqlSessionCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo);
            List<String> list = Arrays.stream(Strings.splitStringByCommaToArray(indexExpr)).toList();
            assertThat(list.size(), equalTo(3));
            assertThat(new HashSet<>(list), equalTo(Strings.commaDelimitedListToSet("logs*,remote1:*,remote1:foo")));
        }

        // two clusters marked as skipped, so only local cluster present in revised index expression
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(
                remote1Alias,
                (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*,foo", true, EsqlExecutionInfo.Cluster.Status.SKIPPED)
            );
            executionInfo.swapCluster(
                remote2Alias,
                (k, v) -> new EsqlExecutionInfo.Cluster(
                    remote2Alias,
                    "mylogs1,mylogs2,logs*",
                    true,
                    EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );

            assertThat(EsqlSessionCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo), equalTo("logs*"));
        }

        // only remotes present and all marked as skipped, so in revised index expression should be empty string
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(
                remote1Alias,
                (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*,foo", true, EsqlExecutionInfo.Cluster.Status.SKIPPED)
            );
            executionInfo.swapCluster(
                remote2Alias,
                (k, v) -> new EsqlExecutionInfo.Cluster(
                    remote2Alias,
                    "mylogs1,mylogs2,logs*",
                    true,
                    EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );

            assertThat(EsqlSessionCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo), equalTo(""));
        }
    }

    public void testUpdateExecutionInfoWithUnavailableClusters() {
        final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        final String remote1Alias = "remote1";
        final String remote2Alias = "remote2";

        // skip_unavailable=true clusters are unavailable, both marked as SKIPPED
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", true));

            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            var unvailableClusters = Map.of(remote1Alias, failure, remote2Alias, failure);
            EsqlSessionCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, unvailableClusters);

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(localClusterAlias, remote1Alias, remote2Alias)));
            assertNull(executionInfo.overallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndShardCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.SKIPPED);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.SKIPPED);
        }

        // skip_unavailable=false cluster is unavailable, throws Exception
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            RemoteTransportException e = expectThrows(
                RemoteTransportException.class,
                () -> EsqlSessionCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, Map.of(remote2Alias, failure))
            );
            assertThat(e.status().getStatus(), equalTo(500));
            assertThat(
                e.getDetailedMessage(),
                containsString("Remote cluster [remote2] (with setting skip_unavailable=false) is not available")
            );
            assertThat(e.getCause().getMessage(), containsString("unable to connect"));
        }

        // all clusters available, no Clusters in ExecutionInfo should be modified
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsqlSessionCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, Map.of());

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(localClusterAlias, remote1Alias, remote2Alias)));
            assertNull(executionInfo.overallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndShardCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }
    }

    public void testUpdateExecutionInfoWithClustersWithNoMatchingIndices() {
        final String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        final String remote1Alias = "remote1";
        final String remote2Alias = "remote2";
        // all clusters present in EsIndex, so no updates to EsqlExecutionInfo should happen
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsIndex esIndex = new EsIndex(
                "logs*,remote1:*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",
                randomMapping(),
                Map.of(
                    "logs-a",
                    IndexMode.STANDARD,
                    "remote1:logs-a",
                    IndexMode.STANDARD,
                    "remote2:mylogs1",
                    IndexMode.STANDARD,
                    "remote2:mylogs2",
                    IndexMode.STANDARD,
                    "remote2:logs-b",
                    IndexMode.STANDARD
                )
            );

            IndexResolution indexResolution = IndexResolution.valid(esIndex, esIndex.concreteIndices(), Map.of());

            EsqlSessionCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndShardCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }

        // remote1 is missing from EsIndex info, so it should be updated and marked as SKIPPED with 0 total shards, 0 took time, etc.
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",
                randomMapping(),
                Map.of(
                    "logs-a",
                    IndexMode.STANDARD,
                    "remote2:mylogs1",
                    IndexMode.STANDARD,
                    "remote2:mylogs2",
                    IndexMode.STANDARD,
                    "remote2:logs-b",
                    IndexMode.STANDARD
                )
            );
            IndexResolution indexResolution = IndexResolution.valid(esIndex, esIndex.concreteIndices(), Map.of());

            EsqlSessionCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remote1Cluster.getTook().millis(), equalTo(0L));
            assertThat(remote1Cluster.getTotalShards(), equalTo(0));
            assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
            assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
            assertThat(remote1Cluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }

        // all remotes are missing from EsIndex info, so they should be updated and marked as SKIPPED with 0 total shards, 0 took time, etc.
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", true));

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",
                randomMapping(),
                Map.of("logs-a", IndexMode.STANDARD)
            );
            // remote1 is unavailable
            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            IndexResolution indexResolution = IndexResolution.valid(esIndex, esIndex.concreteIndices(), Map.of(remote1Alias, failure));

            EsqlSessionCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(localClusterAlias);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            // since remote1 is in the unavailable Map (passed to IndexResolution.valid), it's status will not be changed
            // by updateExecutionInfoWithClustersWithNoMatchingIndices (it is handled in updateExecutionInfoWithUnavailableClusters)
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            assertThat(remote2Cluster.getTook().millis(), equalTo(0L));
            assertThat(remote2Cluster.getTotalShards(), equalTo(0));
            assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
            assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
            assertThat(remote2Cluster.getFailedShards(), equalTo(0));
        }

        // all remotes are missing from EsIndex info. Since one is configured with skip_unavailable=false,
        // an exception should be thrown
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*"));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true));
            executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",
                randomMapping(),
                Map.of("logs-a", IndexMode.STANDARD)
            );

            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            IndexResolution indexResolution = IndexResolution.valid(esIndex, esIndex.concreteIndices(), Map.of(remote1Alias, failure));
            VerificationException ve = expectThrows(
                VerificationException.class,
                () -> EsqlSessionCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution)
            );
            assertThat(ve.getDetailedMessage(), containsString("Unknown index [remote2:mylogs1,mylogs2,logs*]"));
        }
    }

    public void testDetermineUnavailableRemoteClusters() {
        // two clusters, both "remote unavailable" type exceptions
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote2:mylogs1" }, new NoSuchRemoteClusterException("remote2")));
            failures.add(
                new FieldCapabilitiesFailure(
                    new String[] { "remote1:foo", "remote1:bar" },
                    new IllegalStateException("Unable to open any connections")
                )
            );

            Map<String, FieldCapabilitiesFailure> unavailableClusters = EsqlSessionCCSUtils.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters.keySet(), equalTo(Set.of("remote1", "remote2")));
        }

        // one cluster with "remote unavailable" with two failures
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote2:mylogs1" }, new NoSuchRemoteClusterException("remote2")));
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote2:mylogs1" }, new NoSeedNodeLeftException("no seed node")));

            Map<String, FieldCapabilitiesFailure> unavailableClusters = EsqlSessionCCSUtils.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters.keySet(), equalTo(Set.of("remote2")));
        }

        // two clusters, one "remote unavailable" type exceptions and one with another type
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote1:mylogs1" }, new CorruptIndexException("foo", "bar")));
            failures.add(
                new FieldCapabilitiesFailure(
                    new String[] { "remote2:foo", "remote2:bar" },
                    new IllegalStateException("Unable to open any connections")
                )
            );
            Map<String, FieldCapabilitiesFailure> unavailableClusters = EsqlSessionCCSUtils.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters.keySet(), equalTo(Set.of("remote2")));
        }

        // one cluster1 with exception not known to indicate "remote unavailable"
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote1:mylogs1" }, new RuntimeException("foo")));
            Map<String, FieldCapabilitiesFailure> unavailableClusters = EsqlSessionCCSUtils.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters.keySet(), equalTo(Set.of()));
        }

        // empty failures list
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            Map<String, FieldCapabilitiesFailure> unavailableClusters = EsqlSessionCCSUtils.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters.keySet(), equalTo(Set.of()));
        }
    }

    public void testUpdateExecutionInfoAtEndOfPlanning() {
        String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        String remote1Alias = "remote1";
        String remote2Alias = "remote2";
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
        executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
        executionInfo.swapCluster(
            remote1Alias,
            (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "*", true, EsqlExecutionInfo.Cluster.Status.SKIPPED)
        );
        executionInfo.swapCluster(remote2Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote2Alias, "mylogs1,mylogs2,logs*", false));

        assertNull(executionInfo.planningTookTime());
        assertNull(executionInfo.overallTook());
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {}

        EsqlSessionCCSUtils.updateExecutionInfoAtEndOfPlanning(executionInfo);

        assertThat(executionInfo.planningTookTime().millis(), greaterThanOrEqualTo(0L));
        assertNull(executionInfo.overallTook());

        // only remote1 should be altered, since it is the only one marked as SKIPPED when passed into updateExecutionInfoAtEndOfPlanning
        EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertNull(localCluster.getTotalShards());
        assertNull(localCluster.getTook());

        EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(remote1Alias);
        assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        assertThat(remote1Cluster.getTotalShards(), equalTo(0));
        assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
        assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
        assertThat(remote1Cluster.getFailedShards(), equalTo(0));
        assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
        assertThat(remote1Cluster.getTook().millis(), equalTo(executionInfo.planningTookTime().millis()));

        EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(remote2Alias);
        assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertNull(remote2Cluster.getTotalShards());
        assertNull(remote2Cluster.getTook());
    }

    private void assertClusterStatusAndShardCounts(EsqlExecutionInfo.Cluster cluster, EsqlExecutionInfo.Cluster.Status status) {
        assertThat(cluster.getStatus(), equalTo(status));
        assertNull(cluster.getTook());
        if (status == EsqlExecutionInfo.Cluster.Status.RUNNING) {
            assertNull(cluster.getTotalShards());
            assertNull(cluster.getSuccessfulShards());
            assertNull(cluster.getSkippedShards());
            assertNull(cluster.getFailedShards());
        } else if (status == EsqlExecutionInfo.Cluster.Status.SKIPPED) {
            assertThat(cluster.getTotalShards(), equalTo(0));
            assertThat(cluster.getSuccessfulShards(), equalTo(0));
            assertThat(cluster.getSkippedShards(), equalTo(0));
            assertThat(cluster.getFailedShards(), equalTo(0));
        } else {
            fail("Unexpected status: " + status);
        }
    }

    private static Map<String, EsField> randomMapping() {
        int size = between(0, 10);
        Map<String, EsField> result = new HashMap<>(size);
        while (result.size() < size) {
            result.put(randomAlphaOfLength(5), EsFieldTests.randomAnyEsField(1));
        }
        return result;
    }

    public void testReturnSuccessWithEmptyResult() {
        String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        String remote1Alias = "remote1";
        String remote2Alias = "remote2";
        String remote3Alias = "remote3";
        NoClustersToSearchException noClustersException = new NoClustersToSearchException();
        Predicate<String> skipUnPredicate = s -> {
            if (s.equals("remote2") || s.equals("remote3")) {
                return true;
            }
            return false;
        };

        EsqlExecutionInfo.Cluster localCluster = new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false);
        EsqlExecutionInfo.Cluster remote1 = new EsqlExecutionInfo.Cluster(remote1Alias, "logs*", false);
        EsqlExecutionInfo.Cluster remote2 = new EsqlExecutionInfo.Cluster(remote2Alias, "logs*", true);
        EsqlExecutionInfo.Cluster remote3 = new EsqlExecutionInfo.Cluster(remote3Alias, "logs*", true);

        // not a cross-cluster cluster search, so do not return empty result
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
            executionInfo.swapCluster(localClusterAlias, (k, v) -> localCluster);
            assertFalse(EsqlSessionCCSUtils.returnSuccessWithEmptyResult(executionInfo, noClustersException));
        }

        // local cluster is present, so do not return empty result
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
            executionInfo.swapCluster(localClusterAlias, (k, v) -> localCluster);
            executionInfo.swapCluster(remote1Alias, (k, v) -> remote1);
            // TODO: this logic will be added in the follow-on PR that handles missing indices
            // assertFalse(EsqlSessionCCSUtils.returnSuccessWithEmptyResult(executionInfo, noClustersException));
        }

        // remote-only, one cluster is skip_unavailable=false, so do not return empty result
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
            executionInfo.swapCluster(remote1Alias, (k, v) -> remote1);
            executionInfo.swapCluster(remote2Alias, (k, v) -> remote2);
            assertFalse(EsqlSessionCCSUtils.returnSuccessWithEmptyResult(executionInfo, noClustersException));
        }

        // remote-only, all clusters are skip_unavailable=true, so should return empty result with
        // NoSuchClustersException or "remote unavailable" type exception
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
            executionInfo.swapCluster(remote2Alias, (k, v) -> remote2);
            executionInfo.swapCluster(remote3Alias, (k, v) -> remote3);
            Exception e = randomFrom(
                new NoSuchRemoteClusterException("foo"),
                noClustersException,
                new NoSeedNodeLeftException("foo"),
                new IllegalStateException("unknown host")
            );
            assertTrue(EsqlSessionCCSUtils.returnSuccessWithEmptyResult(executionInfo, e));
        }

        // remote-only, all clusters are skip_unavailable=true, but exception is not "remote unavailable" so return false
        // Note: this functionality may change in follow-on PRs, so remove this test in that case
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
            executionInfo.swapCluster(remote2Alias, (k, v) -> remote2);
            executionInfo.swapCluster(remote3Alias, (k, v) -> remote3);
            assertFalse(EsqlSessionCCSUtils.returnSuccessWithEmptyResult(executionInfo, new NullPointerException()));
        }
    }

    public void testUpdateExecutionInfoToReturnEmptyResult() {
        String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        String remote1Alias = "remote1";
        String remote2Alias = "remote2";
        String remote3Alias = "remote3";
        ConnectTransportException transportEx = new ConnectTransportException(null, "foo");
        Predicate<String> skipUnPredicate = s -> {
            if (s.startsWith("remote")) {
                return true;
            }
            return false;
        };

        EsqlExecutionInfo.Cluster localCluster = new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false);
        EsqlExecutionInfo.Cluster remote1 = new EsqlExecutionInfo.Cluster(remote1Alias, "logs*", true);
        EsqlExecutionInfo.Cluster remote2 = new EsqlExecutionInfo.Cluster(remote2Alias, "logs*", true);
        EsqlExecutionInfo.Cluster remote3 = new EsqlExecutionInfo.Cluster(remote3Alias, "logs*", true);

        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
        executionInfo.swapCluster(localCluster.getClusterAlias(), (k, v) -> localCluster);
        executionInfo.swapCluster(remote1.getClusterAlias(), (k, v) -> remote1);
        executionInfo.swapCluster(remote2.getClusterAlias(), (k, v) -> remote2);
        executionInfo.swapCluster(remote3.getClusterAlias(), (k, v) -> remote3);

        assertNull(executionInfo.overallTook());

        EsqlSessionCCSUtils.updateExecutionInfoToReturnEmptyResult(executionInfo, transportEx);

        assertNotNull(executionInfo.overallTook());
        assertThat(executionInfo.getCluster(localClusterAlias).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertThat(executionInfo.getCluster(localClusterAlias).getFailures().size(), equalTo(0));

        for (String remoteAlias : Set.of(remote1Alias, remote2Alias, remote3Alias)) {
            assertThat(executionInfo.getCluster(remoteAlias).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            List<ShardSearchFailure> remoteFailures = executionInfo.getCluster(remoteAlias).getFailures();
            assertThat(remoteFailures.size(), equalTo(1));
            assertThat(remoteFailures.get(0).reason(), containsString("unable to connect to remote cluster"));
        }
    }

    public void testMissingIndicesIsFatal() {
        String localClusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
        String remote1Alias = "remote1";
        String remote2Alias = "remote2";
        String remote3Alias = "remote3";

        // scenario 1: cluster is skip_unavailable=true - not fatal
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "mylogs1,mylogs2,logs*", true));
            assertThat(EsqlSessionCCSUtils.missingIndicesIsFatal(remote1Alias, executionInfo), equalTo(false));
        }

        // scenario 2: cluster is local cluster and had no concrete indices - not fatal
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "logs*", false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "mylogs1,mylogs2,logs*", true));
            assertThat(EsqlSessionCCSUtils.missingIndicesIsFatal(localClusterAlias, executionInfo), equalTo(false));
        }

        // scenario 3: cluster is local cluster and user specified a concrete index - fatal
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            String localIndexExpr = randomFrom("foo*,logs", "logs", "logs,metrics", "bar*,x*,logs", "logs-1,*x*");
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, localIndexExpr, false));
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, "mylogs1,mylogs2,logs*", true));
            assertThat(EsqlSessionCCSUtils.missingIndicesIsFatal(localClusterAlias, executionInfo), equalTo(true));
        }

        // scenario 4: cluster is skip_unavailable=false - always fatal
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(localClusterAlias, (k, v) -> new EsqlExecutionInfo.Cluster(localClusterAlias, "*", false));
            String indexExpr = randomFrom("foo*,logs", "logs", "bar*,x*,logs", "logs-1,*x*", "*");
            executionInfo.swapCluster(remote1Alias, (k, v) -> new EsqlExecutionInfo.Cluster(remote1Alias, indexExpr, false));
            assertThat(EsqlSessionCCSUtils.missingIndicesIsFatal(remote1Alias, executionInfo), equalTo(true));
        }

    }

    public void testCheckForCcsLicense() {
        final TestIndicesExpressionGrouper indicesGrouper = new TestIndicesExpressionGrouper();

        // this seems to be used only for tracking usage of features, not for checking if a license is expired
        final LongSupplier currTime = () -> System.currentTimeMillis();

        XPackLicenseState enterpriseLicenseValid = new XPackLicenseState(currTime, activeLicenseStatus(License.OperationMode.ENTERPRISE));
        XPackLicenseState trialLicenseValid = new XPackLicenseState(currTime, activeLicenseStatus(License.OperationMode.TRIAL));
        XPackLicenseState platinumLicenseValid = new XPackLicenseState(currTime, activeLicenseStatus(License.OperationMode.PLATINUM));
        XPackLicenseState goldLicenseValid = new XPackLicenseState(currTime, activeLicenseStatus(License.OperationMode.GOLD));
        XPackLicenseState basicLicenseValid = new XPackLicenseState(currTime, activeLicenseStatus(License.OperationMode.BASIC));
        XPackLicenseState standardLicenseValid = new XPackLicenseState(currTime, activeLicenseStatus(License.OperationMode.STANDARD));
        XPackLicenseState missingLicense = new XPackLicenseState(currTime, activeLicenseStatus(License.OperationMode.MISSING));
        XPackLicenseState nullLicense = null;

        final XPackLicenseStatus enterpriseStatus = inactiveLicenseStatus(License.OperationMode.ENTERPRISE);
        XPackLicenseState enterpriseLicenseInactive = new XPackLicenseState(currTime, enterpriseStatus);
        XPackLicenseState trialLicenseInactive = new XPackLicenseState(currTime, inactiveLicenseStatus(License.OperationMode.TRIAL));
        XPackLicenseState platinumLicenseInactive = new XPackLicenseState(currTime, inactiveLicenseStatus(License.OperationMode.PLATINUM));
        XPackLicenseState goldLicenseInactive = new XPackLicenseState(currTime, inactiveLicenseStatus(License.OperationMode.GOLD));
        XPackLicenseState basicLicenseInactive = new XPackLicenseState(currTime, inactiveLicenseStatus(License.OperationMode.BASIC));
        XPackLicenseState standardLicenseInactive = new XPackLicenseState(currTime, inactiveLicenseStatus(License.OperationMode.STANDARD));
        XPackLicenseState missingLicenseInactive = new XPackLicenseState(currTime, inactiveLicenseStatus(License.OperationMode.MISSING));

        // local only search does not require an enterprise license
        {
            List<TableInfo> indices = new ArrayList<>();
            indices.add(new TableInfo(new TableIdentifier(EMPTY, null, randomFrom("idx", "idx1,idx2*"))));

            checkForCcsLicense(indices, indicesGrouper, enterpriseLicenseValid);
            checkForCcsLicense(indices, indicesGrouper, platinumLicenseValid);
            checkForCcsLicense(indices, indicesGrouper, goldLicenseValid);
            checkForCcsLicense(indices, indicesGrouper, trialLicenseValid);
            checkForCcsLicense(indices, indicesGrouper, basicLicenseValid);
            checkForCcsLicense(indices, indicesGrouper, standardLicenseValid);
            checkForCcsLicense(indices, indicesGrouper, missingLicense);
            checkForCcsLicense(indices, indicesGrouper, nullLicense);

            checkForCcsLicense(indices, indicesGrouper, enterpriseLicenseInactive);
            checkForCcsLicense(indices, indicesGrouper, platinumLicenseInactive);
            checkForCcsLicense(indices, indicesGrouper, goldLicenseInactive);
            checkForCcsLicense(indices, indicesGrouper, trialLicenseInactive);
            checkForCcsLicense(indices, indicesGrouper, basicLicenseInactive);
            checkForCcsLicense(indices, indicesGrouper, standardLicenseInactive);
            checkForCcsLicense(indices, indicesGrouper, missingLicenseInactive);
        }

        // cross-cluster search requires a valid (active, non-expired) enterprise license OR a valid trial license
        {
            List<TableInfo> indices = new ArrayList<>();
            final String indexExprWithRemotes = randomFrom("remote:idx", "idx1,remote:idx2*,remote:logs,c*:idx4");
            if (randomBoolean()) {
                indices.add(new TableInfo(new TableIdentifier(EMPTY, null, indexExprWithRemotes)));
            } else {
                indices.add(new TableInfo(new TableIdentifier(EMPTY, null, randomFrom("idx", "idx1,idx2*"))));
                indices.add(new TableInfo(new TableIdentifier(EMPTY, null, indexExprWithRemotes)));
            }

            // licenses that work
            checkForCcsLicense(indices, indicesGrouper, enterpriseLicenseValid);
            checkForCcsLicense(indices, indicesGrouper, trialLicenseValid);

            // all others fail ---

            // active non-expired non-Enterprise non-Trial licenses
            assertLicenseCheckFails(indices, indicesGrouper, platinumLicenseValid, "active platinum license");
            assertLicenseCheckFails(indices, indicesGrouper, goldLicenseValid, "active gold license");
            assertLicenseCheckFails(indices, indicesGrouper, basicLicenseValid, "active basic license");
            assertLicenseCheckFails(indices, indicesGrouper, standardLicenseValid, "active standard license");
            assertLicenseCheckFails(indices, indicesGrouper, missingLicense, "active missing license");
            assertLicenseCheckFails(indices, indicesGrouper, nullLicense, "none");

            // inactive/expired licenses
            assertLicenseCheckFails(indices, indicesGrouper, enterpriseLicenseInactive, "expired enterprise license");
            assertLicenseCheckFails(indices, indicesGrouper, trialLicenseInactive, "expired trial license");
            assertLicenseCheckFails(indices, indicesGrouper, platinumLicenseInactive, "expired platinum license");
            assertLicenseCheckFails(indices, indicesGrouper, goldLicenseInactive, "expired gold license");
            assertLicenseCheckFails(indices, indicesGrouper, basicLicenseInactive, "expired basic license");
            assertLicenseCheckFails(indices, indicesGrouper, standardLicenseInactive, "expired standard license");
            assertLicenseCheckFails(indices, indicesGrouper, missingLicenseInactive, "expired missing license");
        }
    }

    private XPackLicenseStatus activeLicenseStatus(License.OperationMode operationMode) {
        return new XPackLicenseStatus(operationMode, true, null);
    }

    private XPackLicenseStatus inactiveLicenseStatus(License.OperationMode operationMode) {
        return new XPackLicenseStatus(operationMode, false, "License Expired 123");
    }

    private void assertLicenseCheckFails(
        List<TableInfo> indices,
        TestIndicesExpressionGrouper indicesGrouper,
        XPackLicenseState licenseState,
        String expectedErrorMessageSuffix
    ) {
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> checkForCcsLicense(indices, indicesGrouper, licenseState)
        );
        assertThat(e.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            e.getMessage(),
            equalTo(
                "A valid Enterprise license is required to run ES|QL cross-cluster searches. License found: " + expectedErrorMessageSuffix
            )
        );
    }

    static class TestIndicesExpressionGrouper implements IndicesExpressionGrouper {
        @Override
        public Map<String, OriginalIndices> groupIndices(IndicesOptions indicesOptions, String[] indexExpressions) {
            final Map<String, OriginalIndices> originalIndicesMap = new HashMap<>();
            final String localKey = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

            for (String expr : indexExpressions) {
                assertFalse(Strings.isNullOrBlank(expr));
                String[] split = expr.split(":", 2);
                assertTrue("Bad index expression: " + expr, split.length < 3);
                String clusterAlias;
                String indexExpr;
                if (split.length == 1) {
                    clusterAlias = localKey;
                    indexExpr = expr;
                } else {
                    clusterAlias = split[0];
                    indexExpr = split[1];

                }
                OriginalIndices currIndices = originalIndicesMap.get(clusterAlias);
                if (currIndices == null) {
                    originalIndicesMap.put(clusterAlias, new OriginalIndices(new String[] { indexExpr }, indicesOptions));
                } else {
                    List<String> indicesList = Arrays.stream(currIndices.indices()).collect(Collectors.toList());
                    indicesList.add(indexExpr);
                    originalIndicesMap.put(clusterAlias, new OriginalIndices(indicesList.toArray(new String[0]), indicesOptions));
                }
            }
            return originalIndicesMap;
        }
    }

}
