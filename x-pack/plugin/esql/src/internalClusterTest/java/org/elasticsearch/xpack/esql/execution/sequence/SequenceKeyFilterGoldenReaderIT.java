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
 * This additional QA overlay does not substitute a matcher replay for reader qualification or adopt the remembered rule.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceKeyFilterGoldenReaderIT extends AbstractSequenceIntegTestCase {
    private static final String INPUT_SHA256 = "a0e5b5228b9b05a1ab17be75c9ecf9ec4e25af6b73a36083a9eaf6ac65f23cee";
    private static final String PLAN_SHA256 = "c057616f13465a0e6ec8fccfcfafa1c268eac5f9fd5013957583a368c6708eef";

    /** Every original history checks its native rows, physical identity and expanded rank in both explicit flag modes. */
    public void testAllFiftyNativeHistoriesAgainstTodayWithKeyFilterOffAndOn() throws Exception {
        Map<String, Object> original = resource("pinned-golden-ki-r2.json", INPUT_SHA256);
        Map<String, Object> plan = resource("golden50-native-plan.json", PLAN_SHA256);
        List<?> originals = list(original.get("cases"));
        List<?> cases = list(plan.get("cases"));
        assertEquals(50, originals.size());
        assertEquals(50, cases.size());
        for (int position = 0; position < 50; position++) {
            Map<String, Object> fixture = object(cases.get(position));
            Map<String, Object> frozen = object(originals.get(position));
            String id = text(fixture.get("id"));
            assertEquals(frozen.get("id"), id);
            assertEquals(object(frozen.get("expected")).get("today"), fixture.get("expected_today_labels"));
            assertEquals(object(frozen.get("counters")).get("today"), fixture.get("fixture_today_counters"));
            assertEquals(number(frozen.get("stages")), number(fixture.get("stages")));
            assertEquals(number(frozen.get("limit")), number(object(fixture.get("pragmas_common")).get("sequence_max_candidates")));
            String index = text(fixture.get("index"));
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
                Observed off = observe(text(fixture.get("query")), fixture, false, true);
                assertReleased();
                Observed on = observe(text(fixture.get("query")), fixture, true, true);
                assertReleased();
                // Identity/rank assertions remain tied to the original JSON, not to results from either engine leg.
                List<List<Object>> expected = expectedFromFrozenRows(frozen);
                assertEquals(fixture.get("expected_today_rows"), expected);
                assertEquals(id + "/OFF", expected, off.rows());
                assertEquals(id + "/ON", expected, on.rows());
                assertEquals(id + "/expression-warning-difference", off.expressionWarnings(), on.expressionWarnings());
                assertEquals(off.status().completedMatches(), on.status().completedMatches());
                assertEquals(off.status().rowsEmitted(), on.status().rowsEmitted());
                if (id.startsWith("G35-")) {
                    assertEquals(List.of(List.of("A3", "B2", "C1")), fixture.get("expected_today_labels"));
                }
                if (id.startsWith("G47-")) {
                    assertEquals(6, number(frozen.get("stages")));
                    assertEquals(131072, number(frozen.get("final_step_limit")));
                }
                logCase(fixture, off, on);
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
        // Lower the frozen adapter sentinel operation to native ES|QL syntax; keep pinned inputs unchanged.
        query = query.replace("NULLIF(encoded_values,\"~\")", "CASE(encoded_values == \"~\", null, encoded_values)");
        String coordinator = randomFrom(internalCluster().getNodeNames());
        Settings.Builder settings = Settings.builder();
        for (var entry : object(fixture.get("pragmas_common")).entrySet()) {
            settings.put(entry.getKey(), entry.getValue().toString());
        }
        settings.put("sequence_key_filter", enabled);
        var request = syncEsqlQueryRequest(query).pragmas(new QueryPragmas(settings.build())).profile(profile).allowPartialResults(false);
        var future = new PlainActionFuture<Observed>();
        client(coordinator).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            // The transport releases this borrowed response after the callback; only copied values escape.
            try {
                if (response.isPartial()) {
                    throw new IllegalStateException("Frozen native history returned a partial response");
                }
                SequenceSourceStatus status = null;
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
                }
                var context = internalCluster().getInstance(TransportService.class, coordinator).getThreadPool().getThreadContext();
                List<String> warnings = context.getResponseHeaders()
                    .getOrDefault("Warning", List.of())
                    .stream()
                    .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
                    .toList();
                List<String> expressionWarnings = warnings.stream().filter(warning -> warning.startsWith("Line ")).toList();
                future.onResponse(new Observed(getValuesList(response), warnings, expressionWarnings, status));
            } catch (Exception failure) {
                future.onFailure(failure);
            }
        }, future::onFailure));
        return future.actionGet(TimeValue.timeValueSeconds(60));
    }

    private static List<List<Object>> expectedFromFrozenRows(Map<String, Object> frozen) {
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
        for (Object value : list(object(frozen.get("expected")).get("today"))) {
            List<Object> output = new ArrayList<>();
            for (Object label : list(value)) {
                output.add(label);
                output.addAll(identities.get(text(label)));
            }
            expected.add(output);
        }
        return expected;
    }

    private void logCase(Map<String, Object> fixture, Observed off, Observed on) throws IOException {
        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.field("id", fixture.get("id"));
            builder.field("oracle", "expected.today");
            builder.field("original_model_counters", fixture.get("fixture_today_counters"));
            builder.field("off_rows", off.rows());
            builder.field("on_rows", on.rows());
            builder.field("off_warnings", off.warnings());
            builder.field("on_warnings", on.warnings());
            builder.field("off_expression_warnings", off.expressionWarnings());
            builder.field("on_expression_warnings", on.expressionWarnings());
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
        List<String> warnings,
        List<String> expressionWarnings,
        @Nullable SequenceSourceStatus status
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
