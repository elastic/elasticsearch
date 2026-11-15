/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactoryBuilder;
import org.elasticsearch.compute.data.BlockFactoryProvider;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.SequenceProvenance;
import org.elasticsearch.xpack.esql.plugin.EsqlPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Native profile toggling preserves literal results while retained mapped output contributes to the held-byte peak. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceRuntimeProfileIT extends AbstractSequenceIntegTestCase {
    private static final String FIRST = "sequence-runtime-profile-a";
    private static final String SECOND = "sequence-runtime-profile-b";
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389000001Z");
    private static final String QUERY = """
        SEQUENCE BY key WITHIN 1s PRIMARY 1
        (FROM sequence-runtime-profile-a METADATA _id | MV_EXPAND payload | WHERE LENGTH(payload)>0
            | EVAL alias_payload=payload | KEEP key,_id,alias_payload),
        (FROM sequence-runtime-profile-b METADATA _id | MV_EXPAND payload | WHERE LENGTH(payload)>0
            | EVAL alias_payload=payload | KEEP key,_id,alias_payload)
        | EVAL first_id=_id, second_id=MEMBER(2,_id),
            first_payload=alias_payload, second_payload=MEMBER(2,alias_payload)
        | KEEP key,first_id,second_id,first_payload,second_payload,sequence_provenance | LIMIT 10
        """;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        assertTrue(plugins.removeIf(EsqlPlugin.class::isAssignableFrom));
        plugins.add(TrackingPlugin.class);
        return plugins;
    }

    /** Keep the real allocator and the existing native block ownership checks. */
    public static class TrackingPlugin extends EsqlPluginWithEnterpriseOrTrialLicense {
        @Override
        protected BlockFactoryProvider blockFactoryProvider(BlockFactoryBuilder builder) {
            return new BlockFactoryProvider(new MockBlockFactory(builder));
        }
    }

    public void testProfileToggleKeepsLiteralMatchesAndCountsRetainedOutputGrowth() {
        for (String index : List.of(FIRST, SECOND)) {
            assertAcked(
                indicesAdmin().prepareCreate(index)
                    .setSettings(indexSettings(1, 0))
                    .setMapping("@timestamp", "type=date_nanos", "key", "type=keyword", "payload", "type=keyword,store=true")
            );
        }
        ensureGreen(FIRST, SECOND);
        String coordinator = Arrays.stream(internalCluster().getNodeNames()).sorted().findFirst().orElseThrow();
        String small = "x".repeat(1_024);
        // 32,768 ASCII bytes would exceed Lucene's single-term limit; 32,000 remains a valid mapped keyword.
        String large = "x".repeat(32_000);
        var peaks = new ArrayList<Long>();
        for (String payload : List.of(small, large)) {
            seed(payload);
            Observed off = observe(coordinator, false, payload);
            Observed on = observe(coordinator, true, payload);
            assertEquals(off.values(), on.values());
            assertNull(off.status());
            SequenceSourceStatus status = on.status();
            assertNotNull(status);
            assertEquals("the mapped selection alias must remain retained", 0L, status.payloadSearchRequests());
            assertEquals(0L, status.payloadRowsFetched());
            assertEquals(2L, status.stageDrivers());
            SequenceSourceStatus.RuntimeProfile runtime = status.runtime();
            assertNotNull(runtime);
            assertEquals(List.of(1L, 1L), runtime.rowsFetched());
            assertEquals(List.of(1L, 1L), runtime.candidatesAdmitted());
            for (long nanos : new long[] {
                runtime.searchWaitNanos(),
                runtime.stageRunNanos(),
                runtime.candidateBuildNanos(),
                runtime.matcherNanos(),
                runtime.conditionNanos(),
                runtime.resultBuildNanos() }) {
                assertTrue("profile durations must be nonnegative", nanos >= 0);
            }
            assertTrue(runtime.peakKeys() > 0);
            assertTrue(runtime.peakBytesHeld() > 0);
            peaks.add(runtime.peakBytesHeld());
        }
        assertTrue(
            "retained output growth must contribute to sequence-held bytes",
            peaks.get(1) >= peaks.get(0) + large.length() - small.length()
        );
    }

    private void seed(String payload) {
        var indexed = client().prepareBulk()
            .add(prepareIndex(FIRST).setId("A").setSource("@timestamp", BASE.toString(), "key", "shared", "payload", payload))
            .add(prepareIndex(SECOND).setId("B").setSource("@timestamp", BASE.plusNanos(1).toString(), "key", "shared", "payload", payload))
            .get();
        assertFalse(indexed.buildFailureMessage(), indexed.hasFailures());
        refresh(FIRST, SECOND);
    }

    private Observed observe(String coordinator, boolean profile, String payload) {
        var request = syncEsqlQueryRequest(QUERY).allowPartialResults(false)
            .profile(profile)
            .pragmas(new QueryPragmas(Settings.builder().put("field_extract_preference", "NONE").build()));
        try (var response = client(coordinator).execute(EsqlQueryAction.INSTANCE, request).actionGet(TimeValue.timeValueSeconds(30))) {
            assertFalse(response.isPartial());
            List<List<Object>> rows = getValuesList(response);
            assertEquals(1, rows.size());
            assertEquals(List.of("shared", "A", "B", payload, payload), rows.getFirst().subList(0, 5));
            assertEquals(6, rows.getFirst().size());
            assertEquals(
                List.of(new SequenceProvenance.Member(1, FIRST, "A"), new SequenceProvenance.Member(2, SECOND, "B")),
                SequenceProvenance.decode(rows.getFirst().get(5))
            );
            if (profile == false) {
                assertNull("profile and its runtime object must be absent when disabled", response.profile());
                return new Observed(rows, null);
            }
            assertNotNull(response.profile());
            List<SequenceSourceStatus> statuses = response.profile()
                .drivers()
                .stream()
                .flatMap(driver -> driver.operators().stream())
                .map(OperatorStatus::status)
                .filter(SequenceSourceStatus.class::isInstance)
                .map(SequenceSourceStatus.class::cast)
                .toList();
            assertEquals(1, statuses.size());
            return new Observed(rows, statuses.getFirst());
        }
    }

    private record Observed(List<List<Object>> values, SequenceSourceStatus status) {}

    /** Queries release their snapshots and owned output independently of profile collection. */
    @After
    public void assertReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
        });
        ensureBlocksReleased();
    }
}
