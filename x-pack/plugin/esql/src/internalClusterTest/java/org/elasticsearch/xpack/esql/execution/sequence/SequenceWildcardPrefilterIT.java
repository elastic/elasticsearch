/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.analysis.common.CommonAnalysisPlugin;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.SequenceProvenance;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2, supportsDedicatedMasters = false, numClientNodes = 0)
public class SequenceWildcardPrefilterIT extends AbstractSequenceIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-01-02T00:00:00Z");
    private static final String START = "sequence-wild-start";
    private static final String END = "sequence-wild-end";
    private static final String KEYWORD = "{\"type\":\"keyword\"}";
    private static final String LIKE = "TO_LOWER(value) LIKE (\"node\", \"python*\")";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), CommonAnalysisPlugin.class);
    }

    public void testRemoteLikeListInFirstStep() {
        assertRemoteList("value LIKE (\"n?de\", \"p?thon*\")", true);
    }

    public void testRemoteLikeListInLaterStep() {
        assertRemoteList("value LIKE (\"n?de\", \"p?thon*\")", false);
    }

    public void testRemoteRlikeListInFirstStep() {
        assertRemoteList("value RLIKE (\"n.de\", \"p.thon.*\")", true);
    }

    public void testRemoteRlikeListInLaterStep() {
        assertRemoteList("value RLIKE (\"n.de\", \"p.thon.*\")", false);
    }

    private void assertRemoteList(String list, boolean inFirstStep) {
        create(START, KEYWORD, KEYWORD, Settings.EMPTY);
        create(END, KEYWORD, KEYWORD, Settings.EMPTY);
        assertFalse(
            client().prepareBulk()
                .add(event(START, "start", "target", "node", 0))
                .add(event(START, "start-noise", "only-before", "other", 1))
                .add(event(START, "miss", "before-only", "node", 1))
                .add(event(END, "end", "target", "python3", 2))
                .add(event(END, "end-noise", "only-after", "other", 3))
                .add(event(END, "miss", "after-only", "python3", 3))
                .get()
                .hasFailures()
        );
        refresh(START, END);
        String where = " | WHERE " + list + " AND TO_LOWER(other) LIKE \"out*\"";
        String query = "SEQUENCE BY key WITHIN 1m PRIMARY 2 (FROM "
            + START
            + " METADATA _id"
            + (inFirstStep ? where : "")
            + "), (FROM "
            + END
            + " METADATA _id"
            + (inFirstStep ? "" : where)
            + ") | KEEP _id, sequence_provenance | LIMIT 20";
        var neither = observe(query, false, false, "DOC_VALUES");
        var wildcard = observe(query, false, true, "DOC_VALUES");
        var on = observe(query, true, true, "DOC_VALUES");
        assertSameResults(neither, wildcard, "end");
        assertSameResults(wildcard, on, "end");
        assertEquals(neither.fetched() - 1, wildcard.fetched());
        assertTrue(on.appliedKeys() > 0);
        assertEquals(
            List.of(new SequenceProvenance.Member(1, START, "start"), new SequenceProvenance.Member(2, END, "end")),
            SequenceProvenance.decode(on.rows().getFirst().getLast())
        );
    }

    public void testPositiveListAndSingleAlongsideMixedConjunctsWithKibanaFilter() {
        fixture(KEYWORD, KEYWORD, Settings.EMPTY);
        for (String where : List.of(LIKE, "TO_LOWER(value) LIKE \"n?d*\"")) {
            // Each optional predicate rules out a different hit. An independent computed/numeric condition
            // and the exception/time bool in observe() must allow the resolved source conjunct to be used.
            String query = query(END, " | EVAL n = rank + 1 | WHERE n >= 1 AND " + where);
            for (boolean keys : List.of(false, true)) {
                var off = observe(query, keys, false, "DOC_VALUES");
                var on = observe(query, keys, true, "DOC_VALUES");
                assertSameResults(off, on, "hit");
                assertEquals(keys ? 3 : 4, off.fetched());
                assertEquals(keys ? 2 : 3, on.fetched());
            }
        }
    }

    public void testIndexedLogsdbWildcardKeepsRowsAndRemovesNoise() {
        String indexed = "{\"type\":\"keyword\",\"index\":true}";
        fixture(indexed, indexed, Settings.builder().put("index.mode", "logsdb").build());
        String query = query(END, " | WHERE " + LIKE);
        var off = observe(query, true, false, "DOC_VALUES");
        var on = observe(query, true, true, "DOC_VALUES");
        assertSameResults(off, on, "hit");
        assertEquals(3, off.fetched());
        assertEquals(2, on.fetched());
        assertTrue(on.appliedKeys() > 0);
    }

    public void testNegationOrAndOverwrittenValueKeepOriginalRows() {
        fixture(KEYWORD, KEYWORD, Settings.EMPTY);
        for (String pipeline : List.of(
            " | WHERE NOT (" + LIKE + ")",
            " | WHERE (" + LIKE + ") OR rank == 2",
            " | EVAL value = other | WHERE " + LIKE
        )) {
            String query = query(END, pipeline);
            var off = observe(query, true, false, "NONE");
            var on = observe(query, true, true, "NONE");
            // The only eligible start uses the first qualifying arrival, so all three controls deliberately use
            // an accepted candidate that would be lost if LIKE were pushed against the original value field.
            assertSameResults(off, on, "miss");
            assertEquals(pipeline, off.fetched(), on.fetched());
        }
    }

    public void testRegexLengthAndExpensiveSettingKeepWorkingKeyFilter() {
        fixture(KEYWORD, KEYWORD, Settings.builder().put("index.max_regex_length", 1).build());
        assertKeyOnly();
        assertAcked(indicesAdmin().prepareUpdateSettings(END).setSettings(Settings.builder().put("index.max_regex_length", 1000)));
        String setting = "search.allow_expensive_queries";
        assertAcked(
            clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                .setTransientSettings(Settings.builder().put(setting, false))
        );
        try {
            assertKeyOnly();
        } finally {
            assertAcked(
                clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
                    .setTransientSettings(Settings.builder().putNull(setting))
            );
        }
    }

    private void assertKeyOnly() {
        String query = query(END, " | WHERE " + LIKE);
        var off = observe(query, true, false, "NONE");
        var on = observe(query, true, true, "NONE");
        assertSameResults(off, on, "hit");
        assertEquals(3, on.fetched());
        assertTrue(on.appliedKeys() > 0);
    }

    public void testUnindexedKeyStillAllowsWildcard() {
        fixture("{\"type\":\"keyword\",\"index\":false}", KEYWORD, Settings.EMPTY);
        String query = query(END, " | WHERE " + LIKE);
        var off = observe(query, true, false, "NONE");
        var on = observe(query, true, true, "NONE");
        assertSameResults(off, on, "hit");
        assertEquals(4, off.fetched());
        assertEquals(3, on.fetched());
        assertEquals(0, on.appliedKeys());
    }

    public void testMixedRegexpMappingsAndSearchNormalizer() {
        String good = END + "-good";
        String bad = END + "-normalized";
        create(START, KEYWORD, KEYWORD, Settings.EMPTY);
        create(good, KEYWORD, KEYWORD, Settings.EMPTY);
        // Indexing NODE preserves it. Normalizing generated [nN][oO][dD][eE] would change the query syntax.
        Settings normalizer = Settings.builder()
            .put("index.analysis.char_filter.brackets.type", "pattern_replace")
            .put("index.analysis.char_filter.brackets.pattern", "\\[|\\]")
            .put("index.analysis.char_filter.brackets.replacement", "")
            .put("index.analysis.normalizer.strip.type", "custom")
            .putList("index.analysis.normalizer.strip.char_filter", "brackets")
            .build();
        create(bad, KEYWORD, "{\"type\":\"keyword\",\"normalizer\":\"strip\"}", normalizer);
        for (String index : List.of(good, bad)) {
            String key = index.substring(END.length() + 1);
            assertFalse(
                client().prepareBulk()
                    .add(event(START, "start-" + key, key, "unused", 0))
                    .add(event(index, "hit-" + key, key, "NODE", 1))
                    .add(event(index, "miss-" + key, key, "outside", 2))
                    .get()
                    .hasFailures()
            );
        }
        refresh(START, good, bad);
        String query = query(END + "-*", " | WHERE " + LIKE);
        var off = observe(query, true, false, "NONE");
        var on = observe(query, true, true, "NONE");
        assertSameResults(off, on, "hit-good", "hit-normalized");
        assertEquals(6, off.fetched());
        assertEquals(5, on.fetched());
        assertTrue(on.appliedKeys() > 0);
    }

    public void testHistoricalIgnoreAboveWithStoredAndDocValues() {
        fixture(KEYWORD, "{\"type\":\"keyword\",\"ignore_above\":3}", Settings.EMPTY);
        assertAcked(
            indicesAdmin().preparePutMapping(END)
                .setSource(Map.of("properties", Map.of("value", Map.of("type", "keyword", "ignore_above", 100))))
        );
        String query = query(END, " | WHERE " + LIKE);
        for (String preference : List.of("STORED", "NONE", "DOC_VALUES")) {
            var off = observe(query, false, false, preference);
            var on = observe(query, false, true, preference);
            if ("STORED".equals(preference)) {
                assertSameResults(off, on, "hit");
                assertEquals(off.fetched(), on.fetched());
            } else {
                assertSameResults(off, on);
            }
        }
    }

    public void testUnicodeExpansionSurvivesSinglePattern() {
        fixture(KEYWORD, KEYWORD, Settings.EMPTY);
        event(END, "unicode", "target", "\u0130", 3).get();
        refresh(END);
        String query = query(END, " | WHERE TO_LOWER(value) LIKE \"i?\"");
        assertSameResults(observe(query, true, false, "NONE"), observe(query, true, true, "NONE"), "unicode");
    }

    private void fixture(String keyMapping, String valueMapping, Settings settings) {
        create(START, KEYWORD, KEYWORD, Settings.EMPTY);
        create(END, keyMapping, valueMapping, settings);
        assertFalse(
            client().prepareBulk()
                .add(event(START, "start", "target", "unused", 0))
                .add(event(END, "miss", "target", "outside", 1))
                .add(event(END, "foreign", "foreign", "NODE", 2))
                .add(event(END, "hit", "target", "NODE", 3))
                .get()
                .hasFailures()
        );
        refresh(START, END);
    }

    private void create(String index, String key, String value, Settings settings) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    indexSettings(1, 0).put("index.mode", "standard")
                        .put("index.routing.allocation.require._name", internalCluster().getNodeNames()[0])
                        .put(settings)
                )
                .setMapping(
                    """
                        {"properties":{"@timestamp":{"type":"date_nanos"},"key":%s,"value":%s,"rank":{"type":"integer"},"other":{"type":"keyword"}}}
                        """
                        .formatted(key, value)
                )
        );
        ensureGreen(index);
    }

    private IndexRequestBuilder event(String index, String id, String key, String value, int offset) {
        return prepareIndex(index).setId(id)
            .setSource(
                "@timestamp",
                BASE.plusSeconds(offset).toString(),
                "key",
                key,
                "value",
                value,
                "rank",
                2,
                "other",
                id.equals("miss") ? "NODE" : "outside"
            );
    }

    private String query(String end, String pipeline) {
        return "SEQUENCE BY key WITHIN 1m PRIMARY 2 (FROM "
            + START
            + " METADATA _id), (FROM "
            + end
            + " METADATA _id"
            + pipeline
            + ") | KEEP _id, sequence_provenance | SORT _id | LIMIT 20";
    }

    private Observed observe(String query, boolean keys, boolean wildcard, String preference) {
        String coordinator = internalCluster().getNodeNames()[1];
        var settings = Settings.builder()
            .put("sequence_key_filter", keys)
            .put("sequence_key_filter_shard_profile", true)
            .put("sequence_wildcard_prefilter", wildcard)
            .put("field_extract_preference", preference)
            .build();
        var request = syncEsqlQueryRequest(query).allowPartialResults(false)
            .profile(true)
            .pragmas(new QueryPragmas(settings))
            .filter(
                boolQuery().filter(rangeQuery("@timestamp").gte(BASE.toString()).lt(BASE.plusSeconds(60).toString()))
                    .filter(boolQuery().mustNot(termQuery("key", "excepted")))
            );
        request.locale(Locale.ROOT);
        var future = new PlainActionFuture<Observed>();
        client(coordinator).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            if (response.isPartial() || response.profile() == null) {
                throw new IllegalStateException("expected complete profiled response");
            }
            long fetched = response.profile()
                .drivers()
                .stream()
                .flatMap(driver -> driver.operators().stream())
                .map(OperatorStatus::status)
                .filter(SequenceSourceStatus.class::isInstance)
                .map(SequenceSourceStatus.class::cast)
                .mapToLong(SequenceSourceStatus::candidateRowsFetched)
                .findFirst()
                .orElseThrow();
            var warnings = internalCluster().getInstance(TransportService.class, coordinator)
                .getThreadPool()
                .getThreadContext()
                .getResponseHeaders()
                .getOrDefault("Warning", List.of())
                .stream()
                .map(w -> HeaderWarning.extractWarningValueFromWarningHeader(w, false))
                .toList();
            var keyProfile = response.getExecutionInfo().queryProfile().sequenceKeyFilterSnapshot();
            long applied = keyProfile == null ? 0 : ((Number) keyProfile.get("applied_shard_searches")).longValue();
            future.onResponse(new Observed(getValuesList(response), SequenceKeyFilterIT.queryWarnings(warnings), fetched, applied));
        }, future::onFailure));
        return future.actionGet(DEFAULT_REQUEST_TIMEOUT);
    }

    private static void assertSameResults(Observed off, Observed on, String... ids) {
        assertEquals(List.of(ids), off.rows().stream().map(List::getFirst).toList());
        for (var row : off.rows()) {
            var members = SequenceProvenance.decode(row.getLast());
            assertEquals(2, members.size());
            assertEquals(row.getFirst(), members.getLast().id());
        }
        assertEquals(off.rows(), on.rows());
        assertEquals(off.warnings(), on.warnings());
    }

    private record Observed(List<List<Object>> rows, List<String> warnings, long fetched, long appliedKeys) {}
}
