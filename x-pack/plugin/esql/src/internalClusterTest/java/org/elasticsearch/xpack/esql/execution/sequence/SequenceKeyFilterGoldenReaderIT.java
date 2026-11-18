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
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryTask;
import org.elasticsearch.xpack.esql.action.SequenceProvenance;
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

/** The G41 busy-installer history pins the selected members and their provenance under ordinary reader defaults. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceKeyFilterGoldenReaderIT extends AbstractSequenceIntegTestCase {
    private static final String INPUT_SHA256 = "f5dabc5de949e17262da43936c077b49ed25a42845e770a4bb33907befc74e9d";
    private static final String PLAN_SHA256 = "cbaf43c39b8966225876b3f594a3741a2b9240da8fbd26dfae71c104ef4f1c1f";

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
            Observed observed = observe(query);
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

    private Observed observe(String query) {
        // Lower the frozen adapter sentinel operation to native ES|QL syntax; keep pinned inputs unchanged.
        query = query.replace("NULLIF(encoded_values,\"~\")", "CASE(encoded_values == \"~\", null, encoded_values)");
        String coordinator = randomFrom(internalCluster().getNodeNames());
        var request = syncEsqlQueryRequest(query).profile(true).allowPartialResults(false);
        var future = new PlainActionFuture<Observed>();
        client(coordinator).execute(EsqlQueryAction.INSTANCE, request, ActionListener.wrap(response -> {
            // The transport releases this borrowed response after the callback; only copied values escape.
            try {
                if (response.isPartial()) {
                    throw new IllegalStateException("Frozen native history returned a partial response");
                }
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
                var context = internalCluster().getInstance(TransportService.class, coordinator).getThreadPool().getThreadContext();
                List<String> expressionWarnings = context.getResponseHeaders()
                    .getOrDefault("Warning", List.of())
                    .stream()
                    .map(warning -> HeaderWarning.extractWarningValueFromWarningHeader(warning, false))
                    .filter(warning -> warning.startsWith("Line "))
                    .toList();
                List<List<Object>> values = getValuesList(response);
                var provenance = values.stream().map(row -> SequenceProvenance.decode(row.getLast())).toList();
                List<List<Object>> rows = values.stream().map(row -> row.subList(0, row.size() - 1)).toList();
                future.onResponse(new Observed(rows, provenance, expressionWarnings, statuses.getFirst()));
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
        List<String> expressionWarnings,
        SequenceSourceStatus status
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
