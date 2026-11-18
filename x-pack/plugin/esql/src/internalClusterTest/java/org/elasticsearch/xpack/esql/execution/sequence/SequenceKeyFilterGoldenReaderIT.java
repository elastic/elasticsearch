/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryTask;
import org.elasticsearch.xpack.esql.action.SequenceProvenance;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

/**
 * The frozen fifty histories reach the native transport, PIT and stage readers with one physical document per event.
 * The reader cases use native candidate filtering and the frozen row identity and expansion-rank oracle.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceKeyFilterGoldenReaderIT extends AbstractSequenceIntegTestCase {
    private static final String INPUT_SHA256 = "a0e5b5228b9b05a1ab17be75c9ecf9ec4e25af6b73a36083a9eaf6ac65f23cee";
    private static final String PLAN_SHA256 = "c057616f13465a0e6ec8fccfcfafa1c268eac5f9fd5013957583a368c6708eef";

    public void testNativeKiHistoriesWithKeyFilterOffAndOn() throws Exception {
        assertGoldenHistories();
    }

    public void testBusyInstallerG41AlertsUsingOrdinaryDefaults() throws Exception {
        String id = "G41-busy-installer-with-expansion";
        Map<String, Object> fixture = pinnedCase("golden50-native-plan.json", PLAN_SHA256, id);
        Map<String, Object> frozen = pinnedCase("ki-r2-golden.json", INPUT_SHA256, id);
        assertEquals(8, number(frozen.get("limit")));
        assertEquals(List.of(List.of("dl", "run", "install")), object(frozen.get("expected")).get("rule"));
        String index = text(fixture.get("index"));
        try {
            createAndSeed(fixture);
            verifyPhysicalEventAndTokenOrder(fixture);
            String query = text(fixture.get("query")).replace(" | LIMIT 1000", ",sequence_provenance | LIMIT 1000");
            Observed observed = observe(query, true, null);
            List<List<Object>> expected = expectedFromFrozenRows(frozen, "rule");
            assertEquals(expected, observed.rows());
            assertEquals(expectedProvenance(expected, index), observed.provenance());
            assertEquals(1L, observed.status().completedMatches());
            assertEquals(1L, observed.status().rowsEmitted());
            assertEquals(List.of(), observed.expressionWarnings());
        } finally {
            if (indexExists(index)) {
                assertAcked(indicesAdmin().prepareDelete(index));
            }
        }
    }

    private Map<String, Object> pinnedCase(String resourceName, String sha256, String id) throws Exception {
        return list(resource(resourceName, sha256).get("cases")).stream()
            .map(SequenceKeyFilterGoldenReaderIT::object)
            .filter(fixture -> id.equals(fixture.get("id")))
            .findFirst()
            .orElseThrow();
    }

    private void assertGoldenHistories() throws Exception {
        Map<String, Object> original = resource("ki-r2-golden.json", INPUT_SHA256);
        Map<String, Object> plan = resource("golden50-native-plan.json", PLAN_SHA256);
        List<?> originals = list(original.get("cases"));
        List<?> cases = list(plan.get("cases"));
        assertEquals(50, originals.size());
        assertEquals(50, cases.size());
        int checked = 0;
        for (int position = 0; position < 50; position++) {
            Map<String, Object> fixture = object(cases.get(position));
            Map<String, Object> frozen = object(originals.get(position));
            String id = text(fixture.get("id"));
            assertEquals(frozen.get("id"), id);
            assertEquals(number(frozen.get("stages")), number(fixture.get("stages")));
            assertEquals(number(frozen.get("limit")), number(object(fixture.get("pragmas_common")).get("sequence_max_candidates")));
            if (number(frozen.get("stages")) > 4) {
                // G47's six-stage work-limit history remains covered by SequenceRememberedGoldenTests.
                assertTrue(id.startsWith("G47-"));
                assertEquals(6, number(frozen.get("stages")));
                assertEquals(131072, number(frozen.get("final_step_limit")));
                continue;
            }
            String index = text(fixture.get("index"));
            // Native queries use the production arrival limit. These three fixtures also pin the
            // expected outcome without their artificial one-visit arrival limit.
            String oracle = frozen.get("arrival_step_limit") == null ? "rule" : "rule_without_arrival_limit";
            Throwable primaryFailure = null;
            try {
                createAndSeed(fixture);
                verifyPhysicalEventAndTokenOrder(fixture);
                for (Object controlValue : list(fixture.get("native_stage_controls"))) {
                    Map<String, Object> control = object(controlValue);
                    Observed actual = observe(text(control.get("query")), fixture, false, false);
                    assertEquals(id + "/native-stage-" + control.get("stage"), control.get("expected"), actual.rows());
                    assertEquals(List.of(), actual.expressionWarnings());
                    assertReleased();
                }
                String query = text(fixture.get("query")).replace(" | LIMIT 1000", ",sequence_provenance | LIMIT 1000");
                Observed off = observe(query, fixture, false, true);
                assertReleased();
                Observed on = observe(query, fixture, true, true);
                assertReleased();
                // Identity/rank assertions remain tied to the original JSON, not to results from either engine leg.
                List<List<Object>> expected = expectedFromFrozenRows(frozen, oracle);
                List<List<SequenceProvenance.Member>> provenance = expectedProvenance(expected, index);
                assertEquals(id + "/OFF provenance", provenance, off.provenance());
                assertEquals(id + "/ON provenance", provenance, on.provenance());
                assertEquals(id + "/OFF", expected, off.rows());
                assertEquals(id + "/ON", expected, on.rows());
                // Random coordinators can contribute internal-cluster node deprecations. Keep them in the raw log.
                List<String> offWarnings = SequenceKeyFilterIT.queryWarnings(off.warnings());
                List<String> onWarnings = SequenceKeyFilterIT.queryWarnings(on.warnings());
                assertTrue(
                    id + "/ON introduced query warnings, off=" + offWarnings + ", on=" + onWarnings,
                    offWarnings.containsAll(onWarnings)
                );
                assertNull(id + "/OFF key filter", off.keyFilterProfile());
                assertKeyFilterApplication(id, on);
                assertEquals(off.status().completedMatches(), on.status().completedMatches());
                assertEquals(off.status().rowsEmitted(), on.status().rowsEmitted());
                logCase(fixture, oracle, off, on);
                checked++;
            } catch (Exception | Error failure) {
                primaryFailure = failure;
                throw failure;
            } finally {
                Throwable cleanupFailure = null;
                try {
                    assertReleased();
                } catch (Exception | Error failure) {
                    cleanupFailure = failure;
                }
                try {
                    if (indexExists(index)) {
                        assertAcked(indicesAdmin().prepareDelete(index));
                    }
                } catch (Exception | Error failure) {
                    if (cleanupFailure == null) {
                        cleanupFailure = failure;
                    } else {
                        cleanupFailure.addSuppressed(failure);
                    }
                }
                if (cleanupFailure != null) {
                    if (primaryFailure != null) {
                        primaryFailure.addSuppressed(cleanupFailure);
                    } else if (cleanupFailure instanceof Exception failure) {
                        throw failure;
                    } else if (cleanupFailure instanceof Error failure) {
                        throw failure;
                    } else {
                        throw new AssertionError(cleanupFailure);
                    }
                }
            }
        }
        assertEquals(49, checked);
    }

    private static void assertKeyFilterApplication(String id, Observed on) {
        var profile = on.keyFilterProfile();
        if (id.startsWith("G06-")) {
            // With no start rows, the reader stops before it can dispatch a later-stage search.
            assertEquals(id, 1L, on.status().candidateSearchRequests());
            assertNull(id, profile);
            return;
        }

        assertNotNull(id + "/ON key filter", profile);
        if (id.startsWith("G29-")) {
            // The logical key is computed from the expanded row; the mapped key contains a sentinel.
            assertEquals(id, 0L, profile.get("built_searches"));
            assertEquals(id, 0L, profile.get("applied_shard_searches"));
            assertTrue(id + ": " + profile, object(profile.get("coordinator_fallbacks")).containsKey("non_pushable_key"));
            return;
        }

        // All other native histories have a nonempty first page and an unchanged indexed keyword key.
        assertTrue(id + ": " + profile, profile.get("applied_shard_searches") instanceof Number applied && applied.longValue() > 0);
        assertEquals(id, Map.of(), profile.get("rejected_shard_searches"));
    }

    private void createAndSeed(Map<String, Object> fixture) {
        String index = text(fixture.get("index"));
        Settings.Builder settings = indexSettings(1, 0);
        for (var entry : object(fixture.get("settings")).entrySet()) {
            settings.put(entry.getKey(), entry.getValue().toString());
        }
        assertAcked(indicesAdmin().prepareCreate(index).setSettings(settings).setMapping(object(fixture.get("mappings"))));
        ensureGreen(index);
        var bulk = client().prepareBulk();
        for (Object value : list(fixture.get("documents"))) {
            Map<String, Object> document = object(value);
            bulk.add(prepareIndex(index).setId(text(document.get("id"))).setSource(object(document.get("source"))));
        }
        var response = bulk.get();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        refresh(index);
        assertEquals(0, indicesAdmin().prepareForceMerge(index).setMaxNumSegments(1).setFlush(true).get().getFailedShards());
    }

    private void verifyPhysicalEventAndTokenOrder(Map<String, Object> fixture) {
        String index = text(fixture.get("index"));
        int stages = number(fixture.get("stages"));
        var request = client().prepareSearch(index)
            .setQuery(matchAllQuery())
            .setSize(1000)
            .addSort("@timestamp", SortOrder.ASC)
            .addSort("_doc", SortOrder.ASC)
            .addDocValueField("event_ordinal");
        for (int stage = 1; stage <= stages; stage++) {
            request.addDocValueField("stage_" + stage + "_rows");
        }
        var response = request.get();
        try {
            assertEquals(0, response.getFailedShards());
            var hits = response.getHits().getHits();
            List<?> documents = list(fixture.get("documents"));
            assertEquals(documents.size(), hits.length);
            for (int i = 0; i < hits.length; i++) {
                Map<String, Object> document = object(documents.get(i));
                Map<String, Object> source = object(document.get("source"));
                assertEquals("native timestamp/doc order must realize frozen first-appearance order", document.get("id"), hits[i].getId());
                assertEquals(i, ((Number) hits[i].field("event_ordinal").getValue()).intValue());
                for (int stage = 1; stage <= stages; stage++) {
                    String field = "stage_" + stage + "_rows";
                    if (source.containsKey(field) == false) {
                        // Doc-value fetch can represent an absent cell with an empty DocumentField.
                        assertTrue(hits[i].field(field) == null || hits[i].field(field).getValues().isEmpty());
                    } else {
                        List<String> expected = list(source.get(field)).stream()
                            .map(SequenceKeyFilterGoldenReaderIT::text)
                            .sorted()
                            .toList();
                        assertNotNull(hits[i].field(field));
                        List<String> actual = hits[i].field(field)
                            .getValues()
                            .stream()
                            .map(value -> value instanceof BytesRef bytes ? bytes.utf8ToString() : text(value))
                            .toList();
                        assertEquals("rank tokens must survive native doc values, including equal numeric siblings", expected, actual);
                    }
                }
            }
        } finally {
            response.decRef();
        }
    }

    private Observed observe(String query, Map<String, Object> fixture, boolean enabled, boolean profile) {
        Settings.Builder settings = Settings.builder();
        for (var entry : object(fixture.get("pragmas_common")).entrySet()) {
            settings.put(entry.getKey(), entry.getValue().toString());
        }
        settings.put("sequence_key_filter", enabled);
        return observe(query, profile, new QueryPragmas(settings.build()));
    }

    private Observed observe(String query, boolean profile, @Nullable QueryPragmas pragmas) {
        // Lower the frozen adapter sentinel operation to native ES|QL syntax; keep pinned inputs unchanged.
        query = query.replace("NULLIF(encoded_values,\"~\")", "CASE(encoded_values == \"~\", null, encoded_values)");
        String coordinator = randomFrom(internalCluster().getNodeNames());
        var request = syncEsqlQueryRequest(query).profile(profile).allowPartialResults(false);
        if (pragmas != null) {
            request.pragmas(pragmas);
        }
        var future = new PlainActionFuture<Observed>();
        client(coordinator).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            // The transport releases this borrowed response after the callback; only copied values escape.
            try {
                if (response.isPartial()) {
                    throw new IllegalStateException("Frozen native history returned a partial response");
                }
                SequenceSourceStatus status = null;
                Map<String, Object> keyFilterProfile = null;
                if (profile) {
                    if (response.profile() == null) {
                        throw new IllegalStateException("Native history needs a real profiled reader");
                    }
                    List<SequenceSourceStatus> statuses = response.profile()
                        .drivers()
                        .stream()
                        .flatMap(driver -> driver.operators().stream())
                        .map(OperatorStatus::status)
                        .filter(SequenceSourceStatus.class::isInstance)
                        .map(SequenceSourceStatus.class::cast)
                        .toList();
                    if (statuses.size() != 1 || statuses.getFirst().candidateSearchRequests() <= 0) {
                        throw new IllegalStateException("Native history must execute exactly one real SEQUENCE source");
                    }
                    status = statuses.getFirst();
                    keyFilterProfile = response.getExecutionInfo().queryProfile().sequenceKeyFilterSnapshot();
                }
                var context = internalCluster().getInstance(TransportService.class, coordinator).getThreadPool().getThreadContext();
                List<String> warnings = context.getResponseHeaders()
                    .getOrDefault("Warning", List.of())
                    .stream()
                    .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
                    .toList();
                List<String> expressionWarnings = warnings.stream().filter(warning -> warning.startsWith("Line ")).toList();
                List<List<Object>> values = getValuesList(response);
                List<List<SequenceProvenance.Member>> provenance = profile
                    ? values.stream().map(row -> SequenceProvenance.decode(row.getLast())).toList()
                    : List.of();
                List<List<Object>> rows = profile ? values.stream().map(row -> row.subList(0, row.size() - 1)).toList() : values;
                future.onResponse(new Observed(rows, provenance, warnings, expressionWarnings, status, keyFilterProfile));
            } catch (Exception failure) {
                future.onFailure(failure);
            }
        }, future::onFailure));
        return future.actionGet(TimeValue.timeValueSeconds(60));
    }

    private static List<List<Object>> expectedFromFrozenRows(Map<String, Object> frozen, String oracle) {
        Map<String, List<Object>> identities = new LinkedHashMap<>();
        for (Object value : list(frozen.get("rows"))) {
            Map<String, Object> row = object(value);
            List<Object> identity = List.of(row.get("event"), row.get("row"));
            List<Object> previous = identities.putIfAbsent(text(row.get("label")), identity);
            if (previous != null && previous.equals(identity) == false) {
                throw new IllegalStateException("Frozen label does not identify one physical event/rank");
            }
        }
        List<List<Object>> expected = new ArrayList<>();
        for (Object value : list(object(frozen.get("expected")).get(oracle))) {
            List<Object> output = new ArrayList<>();
            for (Object label : list(value)) {
                output.add(label);
                output.addAll(identities.get(text(label)));
            }
            expected.add(output);
        }
        return expected;
    }

    private static List<List<SequenceProvenance.Member>> expectedProvenance(List<List<Object>> rows, String index) {
        return rows.stream().map(row -> {
            List<SequenceProvenance.Member> members = new ArrayList<>();
            for (int offset = 0; offset < row.size(); offset += 3) {
                members.add(new SequenceProvenance.Member(offset / 3 + 1, index, text(row.get(offset + 1))));
            }
            return members;
        }).toList();
    }

    private void logCase(Map<String, Object> fixture, String oracle, Observed off, Observed on) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field("id", fixture.get("id"));
            builder.field("oracle", "expected." + oracle);
            builder.field("off_rows", off.rows());
            builder.field("on_rows", on.rows());
            builder.field("off_warnings", off.warnings());
            builder.field("on_warnings", on.warnings());
            builder.field("off_expression_warnings", off.expressionWarnings());
            builder.field("on_expression_warnings", on.expressionWarnings());
            builder.field("on_key_filter", on.keyFilterProfile());
            builder.field("off_status");
            off.status().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.field("on_status");
            on.status().toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            logger.info("GOLDEN_NATIVE_CASE {}", BytesReference.bytes(builder).utf8ToString());
        }
    }

    private Map<String, Object> resource(String name, String expectedSha) throws Exception {
        byte[] bytes;
        try (var stream = getClass().getResourceAsStream(name)) {
            if (stream == null) {
                throw new IOException("Missing frozen native fixture [" + name + "]");
            }
            bytes = stream.readAllBytes();
        }
        assertEquals(expectedSha, HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)));
        try (var parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, bytes)) {
            return parser.map();
        }
    }

    private static Map<String, Object> object(Object value) {
        if (value instanceof Map<?, ?> map) {
            Map<String, Object> copy = new LinkedHashMap<>();
            for (var entry : map.entrySet()) {
                copy.put(text(entry.getKey()), entry.getValue());
            }
            return copy;
        }
        throw new IllegalArgumentException("Expected fixture object");
    }

    private static List<?> list(Object value) {
        if (value instanceof List<?> items) {
            return items;
        }
        throw new IllegalArgumentException("Expected fixture array");
    }

    private static String text(Object value) {
        if (value instanceof String text) {
            return text;
        }
        throw new IllegalArgumentException("Expected fixture string");
    }

    private static int number(Object value) {
        if (value instanceof Number number) {
            return Math.toIntExact(number.longValue());
        }
        throw new IllegalArgumentException("Expected fixture integer");
    }

    private record Observed(
        List<List<Object>> rows,
        List<List<SequenceProvenance.Member>> provenance,
        List<String> warnings,
        List<String> expressionWarnings,
        @Nullable SequenceSourceStatus status,
        @Nullable Map<String, Object> keyFilterProfile
    ) {}

    /** Every native call must release PIT contexts, query tasks and allocator ownership, including failed cases. */
    @After
    public void assertReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
            for (TransportService service : internalCluster().getInstances(TransportService.class)) {
                assertFalse(service.getTaskManager().getTasks().values().stream().anyMatch(EsqlQueryTask.class::isInstance));
            }
        });
        ensureBlocksReleased();
    }
}
