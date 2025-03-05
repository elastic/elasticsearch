/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.slm;

import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleMetadata;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;
import org.elasticsearch.xpack.core.slm.action.ExecuteSnapshotLifecycleAction;
import org.elasticsearch.xpack.core.slm.action.GetSnapshotLifecycleStatsAction;
import org.elasticsearch.xpack.core.slm.action.PutSnapshotLifecycleAction;
import org.elasticsearch.xpack.ilm.IndexLifecycle;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class SLMStatsImmutableIT extends AbstractSnapshotIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(LocalStateCompositeXPackPlugin.class, IndexLifecycle.class, SnapshotLifecycle.class, DataStreamsPlugin.class);
    }

    private static final String NEVER_EXECUTE_CRON_SCHEDULE = "* * * 31 FEB ? *";

    public void testSnapshotLifeCycleMetadataEmptyNotChanged() throws Exception {
        final String policyId = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String idxName = "test-idx";
        final String repoName = "test-repo";
        final String snapshot = "snap";

        createRepository(repoName, "fs");

        createSnapshotPolicy(policyId, snapshot, NEVER_EXECUTE_CRON_SCHEDULE, repoName, idxName);
        assertEquals(Map.of(), SnapshotLifecycleMetadata.EMPTY.getStats().getMetrics());

        executePolicy(policyId);

        assertBusy(() -> {
            GetSnapshotLifecycleStatsAction.Response policyStats = getPolicyStats();
            assertNotNull(policyStats.getSlmStats());
            assertTrue(policyStats.getSlmStats().getMetrics().containsKey(policyId));
        });

        assertEquals(Map.of(), SnapshotLifecycleMetadata.EMPTY.getStats().getMetrics());
        disableRepoConsistencyCheck("nothing stored in repo");
    }

    private GetSnapshotLifecycleStatsAction.Response getPolicyStats() {
        try {
            final var req = new AcknowledgedRequest.Plain(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT);
            return client().execute(GetSnapshotLifecycleStatsAction.INSTANCE, req).get();
        } catch (Exception e) {
            fail("failed to get stats");
        }
        return null;
    }

    private void createSnapshotPolicy(String policyName, String snapshotNamePattern, String schedule, String repoId, String indexPattern) {
        logger.info("creating snapshot lifecycle policy: " + policyName);
        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(indexPattern));
        snapConfig.put("ignore_unavailable", false);
        snapConfig.put("partial", true);

        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(
            policyName,
            snapshotNamePattern,
            schedule,
            repoId,
            snapConfig,
            SnapshotRetentionConfiguration.EMPTY
        );

        PutSnapshotLifecycleAction.Request putLifecycle = new PutSnapshotLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            policyName,
            policy
        );
        try {
            client().execute(PutSnapshotLifecycleAction.INSTANCE, putLifecycle).get();
        } catch (Exception e) {
            logger.error("failed to create slm policy", e);
            fail("failed to create policy " + policy + " got: " + e);
        }
    }

    private String executePolicy(String policyId) throws ExecutionException, InterruptedException {
        ExecuteSnapshotLifecycleAction.Request executeReq = new ExecuteSnapshotLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            policyId
        );
        ExecuteSnapshotLifecycleAction.Response resp = client().execute(ExecuteSnapshotLifecycleAction.INSTANCE, executeReq).get();
        return resp.getSnapshotName();
    }
}
