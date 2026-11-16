/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class EsqlExecutionInfoTests extends ESTestCase {

    static final EsqlExecutionInfo.Cluster localCluster = createEsqlExecutionInfoCluster(
        RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY,
        "test"
    );
    static final EsqlExecutionInfo.Cluster remoteCluster = createEsqlExecutionInfoCluster("remote", "test");

    public void testHasMetadataInclude() {
        // includeCCSMetadata + non-local clusters will produce true
        EsqlExecutionInfo info = createEsqlExecutionInfo(true);
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, (k, v) -> localCluster);
        assertFalse(info.hasMetadataToReport());
        info.swapCluster("remote", (k, v) -> remoteCluster);
        assertTrue(info.hasMetadataToReport());
        // Only remote is enough
        info = createEsqlExecutionInfo(true);
        info.swapCluster("remote", (k, v) -> remoteCluster);
        assertTrue(info.hasMetadataToReport());
    }

    public void testHasMetadataIncludeFalse() {
        // If includeCCSMetadata is false, then it should always return false
        EsqlExecutionInfo info = createEsqlExecutionInfo(false);
        assertFalse(info.hasMetadataToReport());
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, (k, v) -> localCluster);
        assertFalse(info.hasMetadataToReport());
        info.swapCluster("remote", (k, v) -> remoteCluster);
        assertFalse(info.hasMetadataToReport());
    }

    public void testHasMetadataPartial() {
        EsqlExecutionInfo info = createEsqlExecutionInfo(false);
        String key = randomFrom(RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY, "remote");
        info.swapCluster(key, (k, v) -> createEsqlExecutionInfoCluster(k, "test", false, EsqlExecutionInfo.Cluster.Status.SUCCESSFUL));
        assertFalse(info.isPartial());
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(key, (k, v) -> createEsqlExecutionInfoCluster(k, "test", false, EsqlExecutionInfo.Cluster.Status.PARTIAL));
        assertTrue(info.isPartial());
        assertFalse(info.hasMetadataToReport());
        info.swapCluster(key, (k, v) -> {
            EsqlExecutionInfo.Cluster.Builder builder = new EsqlExecutionInfo.Cluster.Builder(v);
            builder.addFailures(List.of(new ShardSearchFailure(new IllegalStateException("shard failure"))));
            return builder.build();
        });
        assertTrue(info.hasMetadataToReport());
    }

    public void testStopHookRegisteredAfterMarkAsStoppedFiresImmediately() {
        EsqlExecutionInfo info = createEsqlExecutionInfo(false);
        AtomicBoolean fired = new AtomicBoolean();
        info.markAsStopped();
        info.addStopHook(() -> {
            fired.set(true);
            return true;
        });
        assertTrue("a hook registered after STOP must run immediately, not wait for a second broadcast", fired.get());
    }

    public void testStopHookImmediateTrueIsVisibleToLaterRunStopHooks() {
        EsqlExecutionInfo info = createEsqlExecutionInfo(false);
        AtomicBoolean first = new AtomicBoolean(true);
        info.markAsStopped();
        info.addStopHook(() -> first.getAndSet(false));
        assertFalse("the immediate call consumed the one-shot transition", first.get());
        assertTrue("runStopHooks must still report the cut so STOP can mark is_partial", info.runStopHooks());
        assertTrue(info.isPartial());
    }

    public void testStopHookImmediateTrueMarksPartialAfterRunStopHooksAlreadySampled() {
        EsqlExecutionInfo info = createEsqlExecutionInfo(false);
        info.markAsStopped();
        assertFalse(info.runStopHooks());
        assertFalse(info.isPartial());
        info.addStopHook(() -> true);
        assertTrue("immediate cut after STOP already sampled hooks must still mark is_partial", info.isPartial());
    }

    public void testPlanningBytesAreNotSerialized() throws IOException {
        EsqlExecutionInfo info = createEsqlExecutionInfo(false);
        info.externalPlanning(new ExternalPlanningReservation(NoopCircuitBreaker.INSTANCE));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            info.writeTo(out);
            EsqlExecutionInfo copy = new EsqlExecutionInfo(out.bytes().streamInput());
            assertNull(copy.externalPlanning());
        }
        assertNotNull(info.externalPlanning());
    }

    public static EsqlExecutionInfo createEsqlExecutionInfo(boolean includeCCSMetadata) {
        return new EsqlExecutionInfo(
            Predicates.always(),
            includeCCSMetadata ? EsqlExecutionInfo.IncludeExecutionMetadata.CCS_ONLY : EsqlExecutionInfo.IncludeExecutionMetadata.NEVER
        );
    }

    public static EsqlExecutionInfo.Cluster createEsqlExecutionInfoCluster(String clusterAlias, String indexExpression) {
        return new EsqlExecutionInfo.Cluster(
            clusterAlias,
            clusterAlias,
            indexExpression,
            true,
            EsqlExecutionInfo.Cluster.Status.RUNNING,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    public static EsqlExecutionInfo.Cluster createEsqlExecutionInfoCluster(
        String clusterAlias,
        String indexExpression,
        boolean skipUnavailable,
        EsqlExecutionInfo.Cluster.Status status
    ) {
        return new EsqlExecutionInfo.Cluster(
            clusterAlias,
            clusterAlias,
            indexExpression,
            skipUnavailable,
            status,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }
}
