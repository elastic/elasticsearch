/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.apache.lucene.index.CorruptIndexException;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NoSeedNodeLeftException;
import org.elasticsearch.transport.NoSuchRemoteClusterException;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlExecutionInfo;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
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
import static org.elasticsearch.xpack.esql.session.EsqlCCSUtils.checkForCcsLicense;
import static org.elasticsearch.xpack.esql.session.EsqlCCSUtils.shouldIgnoreRuntimeError;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class EsqlCCSUtilsTests extends ESTestCase {

    private final String LOCAL_CLUSTER_ALIAS = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
    private final String REMOTE1_ALIAS = "remote1";
    private final String REMOTE2_ALIAS = "remote2";

    public void testCreateIndexExpressionFromAvailableClusters() {

        // no clusters marked as skipped
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", true));
            executionInfo.swapCluster(REMOTE2_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "mylogs1,mylogs2,logs*", true));

            String indexExpr = EsqlCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo);
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
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*,foo", true));
            executionInfo.swapCluster(
                REMOTE2_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(
                    REMOTE2_ALIAS,
                    "mylogs1,mylogs2,logs*",
                    true,
                    EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );

            String indexExpr = EsqlCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo);
            List<String> list = Arrays.stream(Strings.splitStringByCommaToArray(indexExpr)).toList();
            assertThat(list.size(), equalTo(3));
            assertThat(new HashSet<>(list), equalTo(Strings.commaDelimitedListToSet("logs*,remote1:*,remote1:foo")));
        }

        // two clusters marked as skipped, so only local cluster present in revised index expression
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
            executionInfo.swapCluster(
                REMOTE1_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*,foo", true, EsqlExecutionInfo.Cluster.Status.SKIPPED)
            );
            executionInfo.swapCluster(
                REMOTE2_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(
                    REMOTE2_ALIAS,
                    "mylogs1,mylogs2,logs*",
                    true,
                    EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );

            assertThat(EsqlCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo), equalTo("logs*"));
        }

        // only remotes present and all marked as skipped, so in revised index expression should be empty string
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(
                REMOTE1_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*,foo", true, EsqlExecutionInfo.Cluster.Status.SKIPPED)
            );
            executionInfo.swapCluster(
                REMOTE2_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(
                    REMOTE2_ALIAS,
                    "mylogs1,mylogs2,logs*",
                    true,
                    EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );

            assertThat(EsqlCCSUtils.createIndexExpressionFromAvailableClusters(executionInfo), equalTo(""));
        }
    }

    public void testUpdateExecutionInfoWithUnavailableClusters() {

        // skip_unavailable=true clusters are unavailable, both marked as SKIPPED
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", true));
            executionInfo.swapCluster(REMOTE2_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "mylogs1,mylogs2,logs*", true));

            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            var unvailableClusters = Map.of(REMOTE1_ALIAS, failure, REMOTE2_ALIAS, failure);
            EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, unvailableClusters);

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER_ALIAS, REMOTE1_ALIAS, REMOTE2_ALIAS)));
            assertNull(executionInfo.overallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER_ALIAS);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE1_ALIAS);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndShardCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.SKIPPED);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE2_ALIAS);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.SKIPPED);
        }

        // skip_unavailable=false cluster is unavailable, throws Exception
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", true));
            executionInfo.swapCluster(
                REMOTE2_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "mylogs1,mylogs2,logs*", false)
            );

            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            RemoteTransportException e = expectThrows(
                RemoteTransportException.class,
                () -> EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, Map.of(REMOTE2_ALIAS, failure))
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
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", true));
            executionInfo.swapCluster(
                REMOTE2_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "mylogs1,mylogs2,logs*", false)
            );

            EsqlCCSUtils.updateExecutionInfoWithUnavailableClusters(executionInfo, Map.of());

            assertThat(executionInfo.clusterAliases(), equalTo(Set.of(LOCAL_CLUSTER_ALIAS, REMOTE1_ALIAS, REMOTE2_ALIAS)));
            assertNull(executionInfo.overallTook());

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER_ALIAS);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE1_ALIAS);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndShardCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE2_ALIAS);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }
    }

    public void testUpdateExecutionInfoWithClustersWithNoMatchingIndices() {

        // all clusters had matching indices from field-caps call, so no updates to EsqlExecutionInfo should happen
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", randomBoolean()));
            executionInfo.swapCluster(
                REMOTE2_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "mylogs1,mylogs2,logs*", randomBoolean())
            );

            EsIndex esIndex = new EsIndex(
                "logs*,remote1:*,remote2:mylogs1,remote2:mylogs2,remote2:logs*", // original user-provided index expression
                randomMapping(),
                Map.of(
                    // resolved indices from field-caps (all clusters represented)
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

            IndexResolution indexResolution = IndexResolution.valid(esIndex, esIndex.concreteIndices(), Set.of(), Map.of());

            EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER_ALIAS);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE1_ALIAS);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertClusterStatusAndShardCounts(remote1Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE2_ALIAS);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }

        // remote1 had no matching indices from field-caps call, it was not marked as unavailable, so it should be updated and
        // marked as SKIPPED with 0 total shards, 0 took time, etc.
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", randomBoolean()));
            executionInfo.swapCluster(
                REMOTE2_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "mylogs1,mylogs2,logs*", randomBoolean())
            );

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",  // original user-provided index expression
                randomMapping(),
                Map.of(
                    // resolved indices from field-caps (none from remote1)
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
            Map<String, FieldCapabilitiesFailure> unavailableClusters = Map.of();
            IndexResolution indexResolution = IndexResolution.valid(esIndex, esIndex.concreteIndices(), Set.of(), unavailableClusters);

            EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER_ALIAS);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE1_ALIAS);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remote1Cluster.getTotalShards(), equalTo(0));
            assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
            assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
            assertThat(remote1Cluster.getFailedShards(), equalTo(0));

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE2_ALIAS);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1,mylogs2,logs*"));
            assertClusterStatusAndShardCounts(remote2Cluster, EsqlExecutionInfo.Cluster.Status.RUNNING);
        }

        // No remotes had matching indices from field-caps call: 1) remote1 because it was unavailable, 2) remote2 was available,
        // but had no matching indices and since no concrete indices were requested, no VerificationException is thrown and is just
        // marked as SKIPPED
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", randomBoolean()));
            executionInfo.swapCluster(
                REMOTE2_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "mylogs1*,mylogs2*,logs*", randomBoolean())
            );

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1*,remote2:mylogs2*,remote2:logs*", // original user-provided index expression
                randomMapping(),
                Map.of("logs-a", IndexMode.STANDARD) // resolved indices from field-caps (none from either remote)
            );
            // remote1 is unavailable
            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            Map<String, FieldCapabilitiesFailure> unavailableClusters = Map.of(REMOTE1_ALIAS, failure);
            IndexResolution indexResolution = IndexResolution.valid(esIndex, esIndex.concreteIndices(), Set.of(), unavailableClusters);

            EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER_ALIAS);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE1_ALIAS);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            // since remote1 is in the unavailable Map (passed to IndexResolution.valid), it's status will not be changed
            // by updateExecutionInfoWithClustersWithNoMatchingIndices (it is handled in updateExecutionInfoWithUnavailableClusters)
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE2_ALIAS);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1*,mylogs2*,logs*"));
            assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
            assertThat(remote2Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
            assertThat(remote2Cluster.getTotalShards(), equalTo(0));
            assertThat(remote2Cluster.getSuccessfulShards(), equalTo(0));
            assertThat(remote2Cluster.getSkippedShards(), equalTo(0));
            assertThat(remote2Cluster.getFailedShards(), equalTo(0));
        }

        // No remotes had matching indices from field-caps call: 1) remote1 because it was unavailable, 2) remote2 was available,
        // but had no matching indices and since a concrete index was requested, a VerificationException is thrown
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*"));
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", randomBoolean()));
            executionInfo.swapCluster(
                REMOTE2_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "mylogs1,mylogs2,logs*", randomBoolean())
            );

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",  // original user-provided index expression
                randomMapping(),
                Map.of("logs-a", IndexMode.STANDARD)  // resolved indices from field-caps (none from either remote)
            );

            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            Map<String, FieldCapabilitiesFailure> unavailableClusters = Map.of(REMOTE1_ALIAS, failure);
            IndexResolution indexResolution = IndexResolution.valid(esIndex, esIndex.concreteIndices(), Set.of(), unavailableClusters);
            VerificationException ve = expectThrows(
                VerificationException.class,
                () -> EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution)
            );
            assertThat(ve.getDetailedMessage(), containsString("Unknown index [remote2:mylogs1,mylogs2,logs*]"));
        }

        // test where remote2 is already marked as SKIPPED so no modifications or exceptions should be thrown
        // (the EsqlSessionCCSUtils.updateExecutionInfoWithUnavailableClusters() method handles that case not the one tested here)
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*"));
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", randomBoolean()));
            // remote2 is already marked as SKIPPED (simulating failed enrich policy lookup due to unavailable cluster)
            executionInfo.swapCluster(
                REMOTE2_ALIAS,
                (k, v) -> new EsqlExecutionInfo.Cluster(
                    REMOTE2_ALIAS,
                    "mylogs1*,mylogs2*,logs*",
                    randomBoolean(),
                    EsqlExecutionInfo.Cluster.Status.SKIPPED
                )
            );

            EsIndex esIndex = new EsIndex(
                "logs*,remote2:mylogs1,remote2:mylogs2,remote2:logs*",  // original user-provided index expression
                randomMapping(),
                Map.of("logs-a", IndexMode.STANDARD)  // resolved indices from field-caps (none from either remote)
            );

            // remote1 is unavailable
            var failure = new FieldCapabilitiesFailure(new String[] { "logs-a" }, new NoSeedNodeLeftException("unable to connect"));
            Map<String, FieldCapabilitiesFailure> unavailableClusters = Map.of(REMOTE1_ALIAS, failure);
            IndexResolution indexResolution = IndexResolution.valid(esIndex, esIndex.concreteIndices(), Set.of(), unavailableClusters);

            EsqlCCSUtils.updateExecutionInfoWithClustersWithNoMatchingIndices(executionInfo, indexResolution);

            EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(LOCAL_CLUSTER_ALIAS);
            assertThat(localCluster.getIndexExpression(), equalTo("logs*"));
            assertClusterStatusAndShardCounts(localCluster, EsqlExecutionInfo.Cluster.Status.RUNNING);

            EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE1_ALIAS);
            assertThat(remote1Cluster.getIndexExpression(), equalTo("*"));
            // since remote1 is in the unavailable Map (passed to IndexResolution.valid), it's status will not be changed
            // by updateExecutionInfoWithClustersWithNoMatchingIndices (it is handled in updateExecutionInfoWithUnavailableClusters)
            assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));

            EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE2_ALIAS);
            assertThat(remote2Cluster.getIndexExpression(), equalTo("mylogs1*,mylogs2*,logs*"));
            assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
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

            Map<String, FieldCapabilitiesFailure> unavailableClusters = EsqlCCSUtils.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters.keySet(), equalTo(Set.of("remote1", "remote2")));
        }

        // one cluster with "remote unavailable" with two failures
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote2:mylogs1" }, new NoSuchRemoteClusterException("remote2")));
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote2:mylogs1" }, new NoSeedNodeLeftException("no seed node")));

            Map<String, FieldCapabilitiesFailure> unavailableClusters = EsqlCCSUtils.determineUnavailableRemoteClusters(failures);
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
            Map<String, FieldCapabilitiesFailure> unavailableClusters = EsqlCCSUtils.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters.keySet(), equalTo(Set.of("remote2")));
        }

        // one cluster1 with exception not known to indicate "remote unavailable"
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            failures.add(new FieldCapabilitiesFailure(new String[] { "remote1:mylogs1" }, new RuntimeException("foo")));
            Map<String, FieldCapabilitiesFailure> unavailableClusters = EsqlCCSUtils.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters.keySet(), equalTo(Set.of()));
        }

        // empty failures list
        {
            List<FieldCapabilitiesFailure> failures = new ArrayList<>();
            Map<String, FieldCapabilitiesFailure> unavailableClusters = EsqlCCSUtils.determineUnavailableRemoteClusters(failures);
            assertThat(unavailableClusters.keySet(), equalTo(Set.of()));
        }
    }

    public void testUpdateExecutionInfoAtEndOfPlanning() {
        String REMOTE1_ALIAS = "remote1";
        String REMOTE2_ALIAS = "remote2";
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
        executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
        executionInfo.swapCluster(
            REMOTE1_ALIAS,
            (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", true, EsqlExecutionInfo.Cluster.Status.SKIPPED)
        );
        executionInfo.swapCluster(REMOTE2_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "mylogs1,mylogs2,logs*", false));
        assertNull(executionInfo.planningTookTime());
        assertNull(executionInfo.overallTook());

        safeSleep(1);

        EsqlCCSUtils.updateExecutionInfoAtEndOfPlanning(executionInfo);

        assertThat(executionInfo.planningTookTime().millis(), greaterThanOrEqualTo(0L));
        assertNull(executionInfo.overallTook());

        // only remote1 should be altered, since it is the only one marked as SKIPPED when passed into updateExecutionInfoAtEndOfPlanning
        EsqlExecutionInfo.Cluster localCluster = executionInfo.getCluster(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY);
        assertThat(localCluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertNull(localCluster.getTotalShards());
        assertNull(localCluster.getTook());

        EsqlExecutionInfo.Cluster remote1Cluster = executionInfo.getCluster(REMOTE1_ALIAS);
        assertThat(remote1Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
        assertThat(remote1Cluster.getTotalShards(), equalTo(0));
        assertThat(remote1Cluster.getSuccessfulShards(), equalTo(0));
        assertThat(remote1Cluster.getSkippedShards(), equalTo(0));
        assertThat(remote1Cluster.getFailedShards(), equalTo(0));
        assertThat(remote1Cluster.getTook().millis(), greaterThanOrEqualTo(0L));
        assertThat(remote1Cluster.getTook().millis(), equalTo(executionInfo.planningTookTime().millis()));

        EsqlExecutionInfo.Cluster remote2Cluster = executionInfo.getCluster(REMOTE2_ALIAS);
        assertThat(remote2Cluster.getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.RUNNING));
        assertNull(remote2Cluster.getTotalShards());
        assertNull(remote2Cluster.getTook());
    }

    private void assertClusterStatusAndShardCounts(EsqlExecutionInfo.Cluster cluster, EsqlExecutionInfo.Cluster.Status status) {
        assertThat(cluster.getStatus(), equalTo(status));
        if (cluster.getTook() != null) {
            // It is also ok if it's null in some tests
            assertThat(cluster.getTook().millis(), greaterThanOrEqualTo(0L));
        }
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
        } else if (status == EsqlExecutionInfo.Cluster.Status.PARTIAL) {
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
        String remote3Alias = "remote3";
        NoClustersToSearchException noClustersException = new NoClustersToSearchException();
        Predicate<String> skipUnPredicate = s -> {
            if (s.equals(REMOTE2_ALIAS) || s.equals("remote3")) {
                return true;
            }
            return false;
        };

        EsqlExecutionInfo.Cluster localCluster = new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false);
        EsqlExecutionInfo.Cluster remote1 = new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "logs*", false);
        EsqlExecutionInfo.Cluster remote2 = new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "logs*", true);
        EsqlExecutionInfo.Cluster remote3 = new EsqlExecutionInfo.Cluster(remote3Alias, "logs*", true);

        // not a cross-cluster cluster search, so do not return empty result
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> localCluster);
            assertFalse(EsqlCCSUtils.returnSuccessWithEmptyResult(executionInfo, noClustersException));
        }

        // local cluster is present, so do not return empty result
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
            executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> localCluster);
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> remote1);
            // TODO: this logic will be added in the follow-on PR that handles missing indices
            // assertFalse(EsqlSessionCCSUtils.returnSuccessWithEmptyResult(executionInfo, noClustersException));
        }

        // remote-only, one cluster is skip_unavailable=false, so do not return empty result
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
            executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> remote1);
            executionInfo.swapCluster(REMOTE2_ALIAS, (k, v) -> remote2);
            assertFalse(EsqlCCSUtils.returnSuccessWithEmptyResult(executionInfo, noClustersException));
        }

        // remote-only, all clusters are skip_unavailable=true, so should return empty result with
        // NoSuchClustersException or "remote unavailable" type exception
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
            executionInfo.swapCluster(REMOTE2_ALIAS, (k, v) -> remote2);
            executionInfo.swapCluster(remote3Alias, (k, v) -> remote3);
            Exception e = randomFrom(
                new NoSuchRemoteClusterException("foo"),
                noClustersException,
                new NoSeedNodeLeftException("foo"),
                new IllegalStateException("unknown host")
            );
            assertTrue(EsqlCCSUtils.returnSuccessWithEmptyResult(executionInfo, e));
        }

        // remote-only, all clusters are skip_unavailable=true, but exception is not "remote unavailable" so return false
        // Note: this functionality may change in follow-on PRs, so remove this test in that case
        {
            EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
            executionInfo.swapCluster(REMOTE2_ALIAS, (k, v) -> remote2);
            executionInfo.swapCluster(remote3Alias, (k, v) -> remote3);
            assertFalse(EsqlCCSUtils.returnSuccessWithEmptyResult(executionInfo, new NullPointerException()));
        }
    }

    public void testUpdateExecutionInfoToReturnEmptyResult() {
        String REMOTE1_ALIAS = "remote1";
        String REMOTE2_ALIAS = "remote2";
        String remote3Alias = "remote3";
        ConnectTransportException transportEx = new ConnectTransportException(null, "foo");
        Predicate<String> skipUnPredicate = s -> {
            if (s.startsWith("remote")) {
                return true;
            }
            return false;
        };

        EsqlExecutionInfo.Cluster localCluster = new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false);
        EsqlExecutionInfo.Cluster remote1 = new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "logs*", true);
        EsqlExecutionInfo.Cluster remote2 = new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "logs*", true);
        EsqlExecutionInfo.Cluster remote3 = new EsqlExecutionInfo.Cluster(remote3Alias, "logs*", true);

        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, randomBoolean());
        executionInfo.swapCluster(localCluster.getClusterAlias(), (k, v) -> localCluster);
        executionInfo.swapCluster(remote1.getClusterAlias(), (k, v) -> remote1);
        executionInfo.swapCluster(remote2.getClusterAlias(), (k, v) -> remote2);
        executionInfo.swapCluster(remote3.getClusterAlias(), (k, v) -> remote3);

        assertNull(executionInfo.overallTook());

        EsqlCCSUtils.updateExecutionInfoToReturnEmptyResult(executionInfo, transportEx);

        assertNotNull(executionInfo.overallTook());
        assertThat(executionInfo.getCluster(LOCAL_CLUSTER_ALIAS).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertThat(executionInfo.getCluster(LOCAL_CLUSTER_ALIAS).getFailures().size(), equalTo(0));

        for (String remoteAlias : Set.of(REMOTE1_ALIAS, REMOTE2_ALIAS, remote3Alias)) {
            assertThat(executionInfo.getCluster(remoteAlias).getStatus(), equalTo(EsqlExecutionInfo.Cluster.Status.SKIPPED));
            List<ShardSearchFailure> remoteFailures = executionInfo.getCluster(remoteAlias).getFailures();
            assertThat(remoteFailures.size(), equalTo(1));
            assertThat(remoteFailures.get(0).reason(), containsString("unable to connect to remote cluster"));
        }
    }

    public void testConcreteIndexRequested() {
        assertThat(EsqlCCSUtils.concreteIndexRequested("logs*"), equalTo(false));
        assertThat(EsqlCCSUtils.concreteIndexRequested("mylogs1,mylogs2,logs*"), equalTo(true));
        assertThat(EsqlCCSUtils.concreteIndexRequested("x*,logs"), equalTo(true));
        assertThat(EsqlCCSUtils.concreteIndexRequested("logs,metrics"), equalTo(true));
        assertThat(EsqlCCSUtils.concreteIndexRequested("*"), equalTo(false));
    }

    public void testCheckForCcsLicense() {
        final TestIndicesExpressionGrouper indicesGrouper = new TestIndicesExpressionGrouper();
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);

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
            List<IndexPattern> indices = List.of(new IndexPattern(EMPTY, randomFrom("idx", "idx1,idx2*")));

            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), enterpriseLicenseValid);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), platinumLicenseValid);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), goldLicenseValid);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), trialLicenseValid);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), basicLicenseValid);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), standardLicenseValid);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), missingLicense);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), nullLicense);

            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), enterpriseLicenseInactive);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), platinumLicenseInactive);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), goldLicenseInactive);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), trialLicenseInactive);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), basicLicenseInactive);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), standardLicenseInactive);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), missingLicenseInactive);
        }

        // cross-cluster search requires a valid (active, non-expired) enterprise license OR a valid trial license
        {
            List<IndexPattern> indices = new ArrayList<>();
            final String indexExprWithRemotes = randomFrom("remote:idx", "idx1,remote:idx2*,remote:logs,c*:idx4");
            if (randomBoolean()) {
                indices.add(new IndexPattern(EMPTY, indexExprWithRemotes));
            } else {
                indices.add(new IndexPattern(EMPTY, randomFrom("idx", "idx1,idx2*")));
                indices.add(new IndexPattern(EMPTY, indexExprWithRemotes));
            }

            // licenses that work
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), enterpriseLicenseValid);
            checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), trialLicenseValid);

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

    public void testShouldIgnoreRuntimeError() {
        Predicate<String> skipUnPredicate = s -> s.equals(REMOTE1_ALIAS);

        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(skipUnPredicate, true);
        executionInfo.swapCluster(LOCAL_CLUSTER_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(LOCAL_CLUSTER_ALIAS, "logs*", false));
        executionInfo.swapCluster(REMOTE1_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE1_ALIAS, "*", true));
        executionInfo.swapCluster(REMOTE2_ALIAS, (k, v) -> new EsqlExecutionInfo.Cluster(REMOTE2_ALIAS, "mylogs1,mylogs2,logs*", false));

        // remote1: skip_unavailable=true, so should ignore connect errors, but not others
        assertThat(
            shouldIgnoreRuntimeError(executionInfo, REMOTE1_ALIAS, new IllegalStateException("Unable to open any connections")),
            is(true)
        );
        assertThat(shouldIgnoreRuntimeError(executionInfo, REMOTE1_ALIAS, new TaskCancelledException("task cancelled")), is(false));
        assertThat(shouldIgnoreRuntimeError(executionInfo, REMOTE1_ALIAS, new ElasticsearchException("something is wrong")), is(false));
        // remote2: skip_unavailable=false, so should not ignore any errors
        assertThat(
            shouldIgnoreRuntimeError(executionInfo, REMOTE2_ALIAS, new IllegalStateException("Unable to open any connections")),
            is(false)
        );
        assertThat(shouldIgnoreRuntimeError(executionInfo, REMOTE2_ALIAS, new TaskCancelledException("task cancelled")), is(false));
        // same for local
        assertThat(
            shouldIgnoreRuntimeError(executionInfo, LOCAL_CLUSTER_ALIAS, new IllegalStateException("Unable to open any connections")),
            is(false)
        );
        assertThat(shouldIgnoreRuntimeError(executionInfo, LOCAL_CLUSTER_ALIAS, new TaskCancelledException("task cancelled")), is(false));
    }

    private XPackLicenseStatus activeLicenseStatus(License.OperationMode operationMode) {
        return new XPackLicenseStatus(operationMode, true, null);
    }

    private XPackLicenseStatus inactiveLicenseStatus(License.OperationMode operationMode) {
        return new XPackLicenseStatus(operationMode, false, "License Expired 123");
    }

    private void assertLicenseCheckFails(
        List<IndexPattern> indices,
        TestIndicesExpressionGrouper indicesGrouper,
        XPackLicenseState licenseState,
        String expectedErrorMessageSuffix
    ) {
        EsqlExecutionInfo executionInfo = new EsqlExecutionInfo(true);
        ElasticsearchStatusException e = expectThrows(
            ElasticsearchStatusException.class,
            () -> checkForCcsLicense(executionInfo, indices, indicesGrouper, Set.of(), licenseState)
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
        public Map<String, OriginalIndices> groupIndices(
            Set<String> remoteClusterNames,
            IndicesOptions indicesOptions,
            String[] indexExpressions
        ) {
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
