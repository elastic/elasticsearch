/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.SequenceProvenance;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/** Real paged readers prove complete waiting-key snapshots and conservative native-mapping fallbacks. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceKeyFilterIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-11-16T00:00:00Z");
    private static final String KEYWORD = "{\"type\":\"keyword\"}";

    // Only these observed internal-cluster node settings vary by coordinator; every raw warning remains logged.
    private static final Set<String> NODE_DEPRECATIONS = Set.of(
        "[indices.merge.scheduler.use_thread_pool] setting was deprecated in Elasticsearch and will be removed in a future release. See the breaking changes documentation for the next major version.",
        "setting [path.shared_data] is deprecated and will be removed in a future release",
        "Configuring multiple [path.data] paths is deprecated. Use RAID or other system level features for utilizing multiple disks. This feature will be removed in a future release.",
        "Configuring [path.data] with a list is deprecated. Instead specify as a string value.",
        "[index.bloom_filter_for_id_field.enabled] setting was deprecated in Elasticsearch and will be removed in a future release. See the deprecation documentation for the next major version."
    );

    /** Use the existing analysis module for the real non-idempotent keyword normalizer. */
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CommonAnalysisPlugin.class);
    }

    /** Unread first pages must stay unfiltered; later EOF allows narrowing while preserving a late start. */
    public void testUnreadFirstPageThenLateStart() {
        String index = "sequence-key-filter-unread";
        create(index, KEYWORD, Settings.EMPTY);
        var bulk = client().prepareBulk();
        for (int i = 0; i < 300; i++) {
            // Four keys keep each native candidate ring below the explicit bound without warning/drop noise.
            bulk.add(event(index, "A" + i, i * 2L, "early-" + (i % 4), "A"));
            bulk.add(event(index, "noise" + i, i * 2L + 1, "outside-" + i, "B"));
        }
        bulk.add(event(index, "late-A", 2000, "late", "A"));
        bulk.add(event(index, "late-B", 2001, "late", "B"));
        assertFalse(bulk.get().hasFailures());
        refresh(index);
        Compared compared = compare(sequence(index, ""), List.of(List.of("late-A", "late-B")));
        assertTrue("a raw first B page must precede complete first-stage EOF", compared.on().status().candidateRowsFetched() >= 558);
        assertTrue("the final complete key set must actually narrow the later B page", fetched(compared.on()) < fetched(compared.off()));
    }

    /** Keys in both future first-branch buffers remain eligible, including a strict same-timestamp pair. */
    public void testBufferedFutureStartAcrossBranchesAndEqualTimestamp() {
        String index = "sequence-key-filter-buffered";
        create(index, KEYWORD, Settings.EMPTY);
        IndexRouting routing = IndexRouting.fromIndexMetadata(clusterService().state().metadata().getProject().index(index));
        String route = "selected";
        int selectedShard = routing.getShard("future-A", route);
        String other = null;
        for (int i = 0; i < 100; i++) {
            String candidate = "outside-" + i;
            if (routing.getShard("noise", candidate) != selectedShard) {
                other = candidate;
                break;
            }
        }
        assertNotNull("fixture must use both physical shards", other);
        var bulk = client().prepareBulk();
        bulk.add(event(index, "old-A", 0, "old", "A", Map.of("part", "one", "target", "never")).setRouting(route));
        // These two equal-timestamp documents share a shard and preserve insertion order in the native _shard_doc tie.
        bulk.add(event(index, "future-A", 500, "new", "A", Map.of("part", "two", "target", "ok")).setRouting(route));
        for (int i = 0; i < 300; i++) {
            bulk.add(event(index, "noise" + i, i + 1, i < 256 ? "old" : "outside-" + i, "B", Map.of("token", "miss")).setRouting(other));
        }
        bulk.add(event(index, "future-B", 500, "new", "B", Map.of("token", "ok")).setRouting(route));
        assertFalse(bulk.get().hasFailures());
        refresh(index);
        String query = """
            SEQUENCE BY key WITHIN 1h PRIMARY 1
            (FROM (FROM %s METADATA _id | WHERE phase=="A" AND part=="one"),
                  (FROM %s METADATA _id | WHERE phase=="A" AND part=="two")),
            (FROM %s METADATA _id | WHERE phase=="B" | WHERE token==MEMBER(1,target))
            | EVAL second_id=MEMBER(2,_id) | KEEP _id,second_id | LIMIT 10
            """.formatted(index, index, index);
        Compared compared = compare(query, List.of(List.of("future-A", "future-B")));
        assertTrue(
            "the snapshot must include the still-buffered new key on the next candidate page",
            fetched(compared.on()) < fetched(compared.off())
        );
    }

    /** A complete128-key set narrows;129keys must fall back for the whole query rather than publish a prefix. */
    public void testTupleCutoffAndShrinkingAfterConsumption() {
        for (int count : List.of(128, 129)) {
            for (boolean shrink : List.of(false, true)) {
                String index = "sequence-key-filter-cutoff-" + count + "-" + shrink;
                create(index, KEYWORD, Settings.EMPTY);
                var bulk = client().prepareBulk();
                for (int i = 0; i < count; i++) {
                    bulk.add(event(index, "A" + i, 0, "key-" + i, "A"));
                }
                if (shrink) {
                    bulk.add(event(index, "early-B", 1, "key-0", "B"));
                }
                for (int i = 0; i < 512; i++) {
                    bulk.add(event(index, "noise" + i, i + 2, "outside-" + i, "B"));
                }
                bulk.add(event(index, "late-B", 2000, "key-1", "B"));
                assertFalse(bulk.get().hasFailures());
                refresh(index);
                List<List<Object>> expected = shrink
                    ? List.of(List.of("A0", "early-B"), List.of("A1", "late-B"))
                    : List.of(List.of("A1", "late-B"));
                Compared compared = compare(sequence(index, ""), expected);
                if (count == 129 && shrink == false) {
                    assertEquals("a129-key snapshot must remain entirely unfiltered", fetched(compared.off()), fetched(compared.on()));
                } else {
                    assertTrue(
                        "consuming the first key may shrink129to128 without changing later EOF",
                        fetched(compared.on()) < fetched(compared.off())
                    );
                }
            }
        }
    }

    /** An exact key predicate may exhaust a later stream with no hits without expiring unrelated starts early. */
    public void testFilteredEofWithoutAnyMatchingEnd() {
        String index = "sequence-key-filter-empty";
        create(index, KEYWORD, Settings.EMPTY);
        var bulk = client().prepareBulk().add(event(index, "A", 0, "waiting", "A"));
        for (int i = 0; i < 512; i++) {
            bulk.add(event(index, "noise" + i, i + 1, "outside-" + i, "B"));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(index);
        Compared compared = compare(sequence(index, ""), List.of());
        assertEquals(1, fetched(compared.on()));
        assertTrue(fetched(compared.on()) < fetched(compared.off()));
        assertNull(compared.off().keyFilterProfile());
        var on = compared.on().keyFilterProfile();
        assertNotNull(on);
        assertEquals(1L, on.get("built_searches"));
        assertEquals(2L, on.get("applied_shard_searches"));
        assertEquals(Map.of(), on.get("rejected_shard_searches"));
        assertEquals(Map.of("standard", 2L), on.get("index_modes"));
    }

    public void testKeyFilterProfileReportsMixedShardDecisions() {
        String eligible = "sequence-key-profile-eligible";
        String fallback = "sequence-key-profile-fallback";
        create(eligible, KEYWORD, Settings.EMPTY);
        create(fallback, "{\"type\":\"keyword\",\"index\":false}", Settings.EMPTY);
        var bulk = client().prepareBulk()
            .add(event(eligible, "A", 0, "match", "A"))
            .add(event(eligible, "B", 10, "match", "B"))
            .add(event(fallback, "C", 20, "unrelated", "B"));
        assertFalse(bulk.get().hasFailures());
        refresh(eligible, fallback);

        Compared compared = compare(sequence("sequence-key-profile-*", ""), List.of(List.of("A", "B")));
        var on = compared.on().keyFilterProfile();
        assertNotNull(on);
        assertEquals(1L, on.get("built_searches"));
        assertEquals(2L, on.get("applied_shard_searches"));
        assertEquals(Map.of("no_indexed_terms", 2L), on.get("rejected_shard_searches"));
        assertEquals(Map.of("standard", 4L), on.get("index_modes"));
    }

    public void testUnrelatedMultivaluedKeyCanBePrunedBeforeValidation() {
        String index = "sequence-key-filter-unrelated-multivalue";
        create(index, KEYWORD, Settings.EMPTY);
        assertFalse(
            client().prepareBulk()
                .add(event(index, "start", 0, "waiting", "A", Map.of("raw", "start-value")))
                .add(event(index, "unrelated", 1, "unused", "B", Map.of("key", List.of("outside-a", "outside-b"))))
                .add(event(index, "end", 2, "waiting", "B", Map.of("raw", "end-value")))
                .get()
                .hasFailures()
        );
        refresh(index);
        String query = """
            SEQUENCE BY key WITHIN 1h PRIMARY 1
            (FROM %s METADATA _id | WHERE phase=="A"), (FROM %s METADATA _id | WHERE phase=="B")
            | EVAL second_id=MEMBER(2,_id),second_raw=MEMBER(2,raw)
            | KEEP key,_id,raw,second_id,second_raw,sequence_provenance | LIMIT 10
            """.formatted(index, index);

        Settings shared = Settings.builder().put("field_extract_preference", "NONE").build();
        var failureWarnings = new AtomicReference<List<String>>();
        Exception failure = expectThrows(Exception.class, () -> observe(query, false, Settings.builder().put(shared), failureWarnings));
        Throwable cause = ExceptionsHelper.unwrapCause(failure);
        assertTrue(cause.toString(), cause instanceof IllegalArgumentException);
        assertEquals("SEQUENCE member [2] BY column [key] is multivalued; use MV_EXPAND inside the stage", cause.getMessage());
        assertEquals(List.of(), queryWarnings(failureWarnings.get()));

        Observed on = observe(query, true, Settings.builder().put(shared), null);
        assertSelectedMembers(
            on,
            List.of("waiting", "start", "start-value", "end", "end-value"),
            List.of(new SequenceProvenance.Member(1, index, "start"), new SequenceProvenance.Member(2, index, "end"))
        );
        assertEquals(List.of(), on.queryWarnings());
        assertEquals(2L, fetched(on));
    }

    public void testUnrelatedMiddleRingCanBePrunedBeforeDefaultLimit() {
        String index = "sequence-key-filter-unrelated-middle";
        create(index, KEYWORD, Settings.EMPTY);
        var bulk = client().prepareBulk().add(event(index, "start", 0, "waiting", "A", Map.of("raw", "start-value")));
        for (int i = 1; i <= 9; i++) {
            bulk.add(event(index, "unrelated-" + i, i, "outside", "B"));
        }
        bulk.add(event(index, "middle", 10, "waiting", "B", Map.of("raw", "middle-value")));
        bulk.add(event(index, "end", 11, "waiting", "C", Map.of("raw", "end-value")));
        assertFalse(bulk.get().hasFailures());
        refresh(index);
        String query = """
            SEQUENCE BY key WITHIN 1h PRIMARY 1
            (FROM %s METADATA _id | WHERE phase=="A"), (FROM %s METADATA _id | WHERE phase=="B"),
            (FROM %s METADATA _id | WHERE phase=="C")
            | EVAL second_id=MEMBER(2,_id),second_raw=MEMBER(2,raw),third_id=MEMBER(3,_id),third_raw=MEMBER(3,raw)
            | KEEP key,_id,raw,second_id,second_raw,third_id,third_raw,sequence_provenance | LIMIT 10
            """.formatted(index, index, index);

        Observed off = observe(query, false, Settings.builder(), null);
        Observed on = observe(query, true, Settings.builder(), null);
        assertSelectedMembers(
            off,
            List.of("waiting", "start", "start-value", "middle", "middle-value", "end", "end-value"),
            List.of(
                new SequenceProvenance.Member(1, index, "start"),
                new SequenceProvenance.Member(2, index, "middle"),
                new SequenceProvenance.Member(3, index, "end")
            )
        );
        assertEquals(off.values(), on.values());
        assertEquals(List.of(), off.queryWarnings());
        assertEquals(off.queryWarnings(), on.queryWarnings());
        assertEquals(0L, off.status().latestValidCounters().candidatesDroppedAtBound());
        assertEquals(0L, on.status().latestValidCounters().candidatesDroppedAtBound());
        assertEquals(12L, fetched(off));
        assertEquals(3L, fetched(on));
    }

    private static void assertSelectedMembers(Observed observed, List<Object> cells, List<SequenceProvenance.Member> members) {
        assertEquals(1, observed.values().size());
        List<Object> row = observed.values().getFirst();
        assertEquals(cells, row.subList(0, row.size() - 1));
        assertEquals(members, SequenceProvenance.decode(row.getLast()));
    }

    /** An EVAL that overwrites a mapped key must not be mistaken for that physical field merely by name. */
    public void testComputedRawNameCollisionAndDateTupleFallBack() {
        for (boolean dateTuple : List.of(false, true)) {
            String index = "sequence-key-filter-computed-" + dateTuple;
            create(index, KEYWORD, Settings.EMPTY);
            var bulk = client().prepareBulk();
            bulk.add(event(index, "A", 0, "wrong-physical-a", "A", Map.of("raw", "ALPHA")));
            for (int i = 0; i < 300; i++) {
                bulk.add(event(index, "noise" + i, i + 1, "wrong-physical-" + i, "B", Map.of("raw", "outside-" + i)));
            }
            bulk.add(event(index, "B", 500, "wrong-physical-b", "B", Map.of("raw", "alpha")));
            assertFalse(bulk.get().hasFailures());
            refresh(index);
            String eval = " | EVAL key=TO_LOWER(raw)" + (dateTuple ? ", bucket=DATE_TRUNC(1s,@timestamp)" : "");
            String query = """
                SEQUENCE BY %s WITHIN 1h PRIMARY 1
                (FROM %s METADATA _id | WHERE phase=="A" %s),
                (FROM %s METADATA _id | WHERE phase=="B" %s)
                | EVAL second_id=MEMBER(2,_id) | KEEP _id,second_id | LIMIT 10
                """.formatted(dateTuple ? "bucket,key" : "key", index, eval, index, eval);
            Compared compared = compare(query, List.of(List.of("A", "B")));
            assertEquals("computed/unsupported components disable the whole predicate", fetched(compared.off()), fetched(compared.on()));
            var profile = compared.on().keyFilterProfile();
            assertNotNull(profile);
            assertEquals(0L, profile.get("built_searches"));
            assertEquals(0L, profile.get("applied_shard_searches"));
            assertTrue(((Map<?, ?>) profile.get("coordinator_fallbacks")).containsKey("non_pushable_key"));
            assertEquals(Map.of(), profile.get("rejected_shard_searches"));
        }
    }

    /** Re-normalizing canonical terms can change them again, so even apparently matching native keywords stay unfiltered. */
    public void testNonIdempotentNormalizerFallsBack() {
        String index = "sequence-key-filter-normalizer";
        Settings settings = Settings.builder()
            .put("index.analysis.filter.double_a.type", "pattern_replace")
            .put("index.analysis.filter.double_a.pattern", "a")
            .put("index.analysis.filter.double_a.replacement", "aa")
            .put("index.analysis.normalizer.double_a.type", "custom")
            .putList("index.analysis.normalizer.double_a.filter", "double_a")
            .build();
        create(index, "{\"type\":\"keyword\",\"normalizer\":\"double_a\"}", settings);
        seedNarrowingFixture(index, "a");
        Compared compared = compare(sequence(index, ""), List.of(List.of("A", "B")));
        assertEquals(fetched(compared.off()), fetched(compared.on()));
    }

    /** A real doc-values-only field cannot safely become an indexed term predicate on its own shard. */
    public void testUnindexedKeyFallsBackAndStoredPreferenceStaysOrdinary() {
        String index = "sequence-key-filter-unindexed";
        create(index, "{\"type\":\"keyword\",\"index\":false}", Settings.EMPTY);
        seedNarrowingFixture(index, "match");
        Compared compared = compare(sequence(index, ""), List.of(List.of("A", "B")));
        assertEquals(fetched(compared.off()), fetched(compared.on()));
        Observed storedOff = observe(sequence(index, ""), false, "STORED");
        Observed storedOn = observe(sequence(index, ""), true, "STORED");
        assertEquals(storedOff.values(), storedOn.values());
        assertEquals(storedOff.queryWarnings(), storedOn.queryWarnings());
        assertEquals(fetched(storedOff), fetched(storedOn));
    }

    /** Historical ignore_above exclusions affect both native terms and sorted-set values before later mapping changes. */
    public void testHistoricalIgnoreAboveUsesTheSameNativeDocValuesIdentity() {
        String index = "sequence-key-filter-ignore-above";
        create(index, "{\"type\":\"keyword\",\"ignore_above\":3}", Settings.EMPTY);
        var bulk = client().prepareBulk();
        bulk.add(event(index, "ignored-A", 0, "too-long", "A"));
        bulk.add(event(index, "ignored-B", 1, "too-long", "B"));
        bulk.add(event(index, "A", 2, "yes", "A"));
        bulk.add(event(index, "B", 500, "yes", "B"));
        for (int i = 0; i < 300; i++) {
            bulk.add(event(index, "noise" + i, i + 3, "n" + (i % 10), "B"));
        }
        assertFalse(bulk.get().hasFailures());
        assertAcked(
            indicesAdmin().preparePutMapping(index).setSource("{\"properties\":{\"key\":{\"type\":\"keyword\",\"ignore_above\":32766}}}")
        );
        refresh(index);
        Compared compared = compare(sequence(index, ""), List.of(List.of("A", "B")));
        assertTrue(fetched(compared.on()) < fetched(compared.off()));
    }

    /** Default requests prune eligible reads and explicit opt-out preserves matching rows. */
    public void testDefaultRequestMatchesExplicitOn() {
        String index = "sequence-key-filter-default";
        create(index, KEYWORD, Settings.EMPTY);
        seedNarrowingFixture(index, "match");
        String query = sequence(index, "");
        Observed off = observe(query, false, "NONE");
        Observed on = observe(query, true, "NONE");
        Observed defaultRequest = observe(query, null, "NONE");
        assertEquals(List.of(List.of("A", "B")), off.values());
        assertEquals(off.values(), on.values());
        assertEquals(on.values(), defaultRequest.values());
        assertTrue("explicit opt-out reads the unrelated keys", fetched(on) < fetched(off));
        assertEquals(fetched(on), fetched(defaultRequest));
        assertEquals(List.of(), defaultRequest.queryWarnings());
    }

    private void seedNarrowingFixture(String index, String key) {
        var bulk = client().prepareBulk().add(event(index, "A", 0, key, "A"));
        for (int i = 0; i < 300; i++) {
            bulk.add(event(index, "noise" + i, i + 1, "outside-" + i, "B"));
        }
        bulk.add(event(index, "B", 500, key, "B"));
        assertFalse(bulk.get().hasFailures());
        refresh(index);
    }

    private void create(String index, String keyMapping, Settings additional) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(indexSettings(2, 0).put("index.mode", "standard").put(additional))
                .setMapping("""
                    {"dynamic":false,"properties":{"@timestamp":{"type":"date_nanos"},"key":%s,
                      "phase":{"type":"keyword"},"raw":{"type":"keyword"},"part":{"type":"keyword"},
                      "target":{"type":"keyword"},"token":{"type":"keyword"}}}
                    """.formatted(keyMapping))
        );
        ensureGreen(index);
    }

    private IndexRequestBuilder event(String index, String id, long millis, String key, String phase) {
        return event(index, id, millis, key, phase, Map.of());
    }

    private IndexRequestBuilder event(String index, String id, long millis, String key, String phase, Map<String, Object> extra) {
        Map<String, Object> body = new HashMap<>();
        body.put("@timestamp", BASE.plusMillis(millis).toString());
        body.put("key", key);
        body.put("phase", phase);
        body.putAll(extra);
        return prepareIndex(index).setId(id).setSource(body);
    }

    private static String sequence(String index, String stageSuffix) {
        return """
            SEQUENCE BY key WITHIN 1h PRIMARY 1
            (FROM %s METADATA _id | WHERE phase=="A" %s),
            (FROM %s METADATA _id | WHERE phase=="B" %s)
            | EVAL second_id=MEMBER(2,_id) | KEEP _id,second_id | SORT _id | LIMIT 10
            """.formatted(index, stageSuffix, index, stageSuffix);
    }

    private Compared compare(String query, List<List<Object>> expected) {
        Observed off = observe(query, false, "NONE");
        Observed on = observe(query, true, "NONE");
        assertEquals(expected, off.values());
        assertEquals(off.values(), on.values());
        assertEquals(List.of(), off.queryWarnings());
        assertEquals(off.queryWarnings(), on.queryWarnings());
        assertEquals(off.status().completedMatches(), on.status().completedMatches());
        assertEquals(off.status().rowsEmitted(), on.status().rowsEmitted());
        assertEquals(off.status().payloadSearchRequests(), on.status().payloadSearchRequests());
        assertEquals(off.status().payloadRowsFetched(), on.status().payloadRowsFetched());
        logger.info("key-filter comparison off={} on={}", off.status(), on.status());
        return new Compared(off, on);
    }

    private Observed observe(String query, @Nullable Boolean enabled, String preference) {
        Settings.Builder settings = Settings.builder().put("field_extract_preference", preference).put("sequence_max_candidates", 128);
        Observed observed = observe(query, enabled, settings, null);
        assertEquals(0L, observed.status().latestValidCounters().candidatesDroppedAtBound());
        assertEquals(0L, observed.status().latestValidCounters().searchesAbandonedAtWorkLimit());
        assertEquals(0L, observed.status().latestValidCounters().keysThatHitBound());
        return observed;
    }

    private Observed observe(
        String query,
        @Nullable Boolean enabled,
        Settings.Builder settings,
        @Nullable AtomicReference<List<String>> failureWarnings
    ) {
        String coordinator = randomFrom(internalCluster().getNodeNames());
        if (enabled != null) {
            settings.put("sequence_key_filter", enabled);
        }
        var future = new PlainActionFuture<Observed>();
        var request = syncEsqlQueryRequest(query).allowPartialResults(false).profile(true).pragmas(new QueryPragmas(settings.build()));
        client(coordinator).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            // The transport releases this borrowed response after the callback; only copied values escape.
            try {
                if (response.isPartial() || response.profile() == null) {
                    throw new IllegalStateException("Key-filter request needs a complete profiled response");
                }
                List<SequenceSourceStatus> statuses = response.profile()
                    .drivers()
                    .stream()
                    .flatMap(driver -> driver.operators().stream())
                    .map(OperatorStatus::status)
                    .filter(SequenceSourceStatus.class::isInstance)
                    .map(SequenceSourceStatus.class::cast)
                    .toList();
                if (statuses.size() != 1) {
                    throw new IllegalStateException("Key-filter request needs exactly one real reader status");
                }
                List<String> warnings = responseWarnings(coordinator);
                var keyFilterProfile = response.getExecutionInfo().queryProfile().sequenceKeyFilterSnapshot();
                future.onResponse(
                    new Observed(getValuesList(response), warnings, queryWarnings(warnings), statuses.getFirst(), keyFilterProfile)
                );
            } catch (Exception failure) {
                future.onFailure(failure);
            }
        }, failure -> {
            if (failureWarnings != null) {
                List<String> warnings = responseWarnings(coordinator);
                failureWarnings.set(warnings);
                logger.info(
                    "key-filter observation enabled={} coordinator={} failure={} warnings={} query_warnings={}",
                    enabled,
                    coordinator,
                    ExceptionsHelper.unwrapCause(failure).getMessage(),
                    warnings,
                    queryWarnings(warnings)
                );
            }
            future.onFailure(failure);
        }));
        Observed observed = future.actionGet(TimeValue.timeValueSeconds(60));
        logger.info(
            "key-filter observation enabled={} coordinator={} values={} warnings={} query_warnings={} node_deprecations={} status={}",
            enabled,
            coordinator,
            observed.values(),
            observed.warnings(),
            observed.queryWarnings(),
            observed.warnings().stream().filter(NODE_DEPRECATIONS::contains).toList(),
            observed.status()
        );
        return observed;
    }

    private List<String> responseWarnings(String coordinator) {
        var context = internalCluster().getInstance(TransportService.class, coordinator).getThreadPool().getThreadContext();
        return context.getResponseHeaders()
            .getOrDefault("Warning", List.of())
            .stream()
            .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
            .toList();
    }

    static List<String> queryWarnings(List<String> warnings) {
        return warnings.stream().filter(warning -> NODE_DEPRECATIONS.contains(warning) == false).toList();
    }

    private static long fetched(Observed observed) {
        return observed.status().candidateRowsFetched();
    }

    private record Observed(
        List<List<Object>> values,
        List<String> warnings,
        List<String> queryWarnings,
        SequenceSourceStatus status,
        @Nullable Map<String, Object> keyFilterProfile
    ) {}

    private record Compared(Observed off, Observed on) {}

    /** Reader EOF, native mapping refusal and callback failures must all release PIT and block ownership. */
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
