/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractPauseFieldPlugin;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.unsignedlong.UnsignedLongMapperPlugin;
import org.junit.After;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.containsString;

/** Differential coverage of mapped field storage layouts using ordinary ES|QL as the value oracle. */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class SequenceMappedFieldsIT extends AbstractEsqlIntegTestCase {
    private static final Instant BASE = Instant.parse("2026-09-25T21:14:51.389000001Z");

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), UnsignedLongMapperPlugin.class),
            FailingRuntimeFieldPlugin.class
        );
    }

    public void testSourceDisabledStoredKeywordAndTextLayouts() {
        Map<String, Object> properties = baseProperties();
        properties.put("key", Map.of("type", "keyword", "doc_values", false, "store", true));
        properties.put("phase", Map.of("type", "keyword", "store", true));
        properties.put("stored_keyword", Map.of("type", "keyword", "doc_values", false, "store", true));
        properties.put("text_own_values", Map.of("type", "text", "doc_values", true));
        properties.put("text_delegate", Map.of("type", "text", "fields", Map.of("raw", Map.of("type", "keyword"))));
        properties.put("text_stored", Map.of("type", "text", "store", true));
        properties.put("text_fielddata", Map.of("type", "text", "fielddata", true));
        properties.put("signed", Map.of("type", "long"));
        properties.put("unsigned", Map.of("type", "unsigned_long"));
        properties.put("ip", Map.of("type", "ip"));
        properties.put("millis", Map.of("type", "date"));
        createIndex("sequence-disabled", Map.of("_source", Map.of("enabled", false), "properties", properties), Settings.EMPTY);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("stored_keyword", List.of("second", "first", "second"));
        values.put("text_own_values", List.of("Complete Phrase", "Another Phrase", "Complete Phrase"));
        values.put("text_delegate", "A Full Phrase With Spaces");
        values.put("text_stored", List.of("Stored Whole Phrase", "Stored Other Phrase"));
        values.put("text_fielddata", "Analyzed Tokens Must Stay Absent");
        values.put("signed", 9_007_199_254_740_993L);
        values.put("unsigned", "18446744073709551615");
        values.put("ip", "2001:db8::1");
        values.put("millis", BASE.toString());
        indexPair("sequence-disabled", values);
        List<String> columns = new ArrayList<>(values.keySet());
        columns.addFirst("key");
        assertOrdinaryParity("sequence-disabled", columns, "");
        // The key itself is stored without doc values; both stages must read it to complete the sequence.
        // Fielddata on a source-disabled text column must not substitute analyzed tokens for the missing original text.
        try (var response = run("from sequence-disabled | where phase==\"A\" | keep text_fielddata | limit 1")) {
            assertNull(getValuesList(response).getFirst().getFirst());
        }
        try (var response = run(syncEsqlQueryRequest("""
            sequence by normalized with {"maxspan":"30s"}
            (from sequence-disabled | where phase=="A" | eval normalized=TO_LOWER(text_delegate)),
            (from sequence-disabled | where phase=="B" | eval normalized=TO_LOWER(text_delegate))
            | keep normalized, stage1.text_delegate | limit 1
            """).pragmas(new QueryPragmas(Settings.EMPTY)))) {
            assertValues(response.values(), List.of(List.of("a full phrase with spaces", "A Full Phrase With Spaces")));
        }
    }

    public void testFlattenedRootReconstructionAndStageExtraction() {
        for (boolean sourceEnabled : List.of(true, false)) {
            String index = "sequence-flattened-" + sourceEnabled;
            var properties = baseProperties();
            properties.put("attributes", Map.of("type", "flattened"));
            if (sourceEnabled == false) {
                properties.put("key", Map.of("type", "keyword", "store", true));
                properties.put("phase", Map.of("type", "keyword", "store", true));
            }
            createIndex(index, Map.of("_source", Map.of("enabled", sourceEnabled), "properties", properties), Settings.EMPTY);
            indexPair(
                index,
                Map.of("attributes", Map.of("product", "Agent", "nested", Map.of("level", 7), "tags", List.of("z", "a", "z")))
            );
            assertOrdinaryParity(index, List.of("key", "attributes", "product"), "| eval product=FIELD_EXTRACT(attributes, \"product\")");
            try (var response = run("from " + index + " | where phase==\"A\" | keep attributes | limit 1")) {
                String value = (String) getValuesList(response).getFirst().getFirst();
                assertThat(value, containsString("product"));
                assertThat(value, containsString("nested"));
            }
        }
    }

    public void testColumnarArraysPreserveOrdinaryOrderDuplicatesAndNullHandling() {
        var properties = baseProperties();
        properties.put("labels", Map.of("type", "keyword"));
        properties.put("text", Map.of("type", "text", "doc_values", true));
        properties.put("numbers", Map.of("type", "long"));
        properties.put("attributes", Map.of("type", "flattened"));
        createIndex(
            "sequence-columnar",
            Map.of("properties", properties),
            Settings.builder().put("index.mode", IndexMode.COLUMNAR.getName()).build()
        );
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("labels", Arrays.asList("z", null, "a", "z"));
        values.put("text", Arrays.asList("Last Phrase", "First Phrase", null, "Last Phrase"));
        values.put("numbers", Arrays.asList(9_007_199_254_740_995L, null, 9_007_199_254_740_993L, 9_007_199_254_740_995L));
        values.put("attributes", Map.of("labels", Arrays.asList("z", null, "a", "z")));
        indexPair("sequence-columnar", values);
        assertOrdinaryParity("sequence-columnar", List.of("key", "labels", "text", "numbers", "attributes"), "");
        // Pin a visible difference from sorted/deduplicated fielddata so the oracle comparison cannot pass vacuously.
        try (var response = run("from sequence-columnar | where phase==\"A\" | keep labels | limit 1")) {
            assertEquals(List.of("z", "a", "z"), getValuesList(response).getFirst().getFirst());
        }
        assertOrdinaryParity(
            "sequence-columnar",
            List.of("key", "label_count", "first_label", "last_number"),
            "| eval label_count=MV_COUNT(labels), first_label=MV_FIRST(labels), last_number=MV_LAST(numbers)"
        );
    }

    public void testMappedLoadingFailureReleasesReadersAndSnapshot() {
        createIndex(
            "sequence-loading-failure",
            Map.of(
                "properties",
                baseProperties(),
                "runtime",
                Map.of("failure", Map.of("type", "long", "script", Map.of("lang", "sequence_failure", "source", "failure")))
            ),
            Settings.EMPTY
        );
        indexPair("sequence-loading-failure", Map.of());
        RuntimeException failure = expectThrows(RuntimeException.class, () -> run(syncEsqlQueryRequest("""
            sequence by key with {"maxspan":"30s"}
            (from sequence-loading-failure | where phase=="A"),
            (from sequence-loading-failure | where phase=="B")
            | keep stage1.failure | limit 1
            """).allowPartialResults(false)));
        assertThat(ExceptionsHelper.stackTrace(failure), containsString("sequence mapped loading failure"));
    }

    /**
     * Injects a deterministic script failure while keeping the real runtime mapper, block loader and fetch path.
     * The existing integration-test script engine avoids a dependency on the full Painless module for this failure fixture.
     */
    public static class FailingRuntimeFieldPlugin extends AbstractPauseFieldPlugin {
        @Override
        protected String scriptTypeName() {
            return "sequence_failure";
        }

        @Override
        protected void onStartExecute() {
            throw new IllegalArgumentException("sequence mapped loading failure");
        }

        @Override
        protected boolean onWait() {
            return true;
        }
    }

    public void testMixedDateMappingRequiresConversionButUnusedPayloadIsPruned() {
        for (String type : List.of("date", "date_nanos")) {
            var properties = baseProperties();
            properties.put("payload_time", Map.of("type", type));
            String index = "sequence-mixed-" + type;
            createIndex(index, Map.of("properties", properties), Settings.EMPTY);
            indexPair(index, Map.of("payload_time", BASE.toString()));
        }
        var pragmas = new QueryPragmas(Settings.EMPTY);
        try (var ordinary = run(syncEsqlQueryRequest("""
            from sequence-mixed-* | where phase=="A"
            | keep payload_time | sort payload_time | limit 10
            """).pragmas(pragmas))) {
            assertEquals(
                List.of(Instant.parse("2026-09-25T21:14:51.389Z"), BASE),
                getValuesList(ordinary).stream().map(row -> Instant.parse((String) row.getFirst())).toList()
            );
        }
        String sequence = """
            sequence by key with {"maxspan":"30s"}
            (from sequence-mixed-* | where phase=="A"),
            (from sequence-mixed-* | where phase=="B")
            """;
        var failure = expectThrows(
            VerificationException.class,
            () -> run(syncEsqlQueryRequest(sequence + " | keep stage1.payload_time | limit 1").pragmas(pragmas))
        );
        assertThat(failure.getMessage(), containsString("source column [payload_time] requiring a conversion across mappings"));
        try (var pruned = run(syncEsqlQueryRequest(sequence + " | keep key | limit 1").pragmas(pragmas))) {
            assertValues(pruned.values(), List.of(List.of("shared")));
        }
    }

    private void assertOrdinaryParity(String index, List<String> columns, String derivation) {
        for (var preference : List.of(MappedFieldType.FieldExtractPreference.NONE, MappedFieldType.FieldExtractPreference.STORED)) {
            var pragmas = new QueryPragmas(Settings.builder().put("field_extract_preference", preference).build());
            for (int stage = 1; stage <= 2; stage++) {
                String phase = stage == 1 ? "A" : "B";
                String ordinary = "from " + index + " | where phase==\"" + phase + "\" " + derivation;
                String sequence = Strings.format("""
                    sequence by key with {"maxspan":"30s"}
                    (from %s | where phase=="A" %s), (from %s | where phase=="B" %s)
                    """, index, derivation, index, derivation);
                String projection = String.join(", ", columns);
                final int stageNumber = stage;
                String qualified = String.join(
                    ", ",
                    columns.stream().map(column -> column.equals("key") ? column : "stage" + stageNumber + "." + column).toList()
                );
                try (
                    var ordinaryResponse = run(syncEsqlQueryRequest(ordinary + " | keep " + projection + " | limit 1").pragmas(pragmas));
                    var sequenceResponse = run(syncEsqlQueryRequest(sequence + " | keep " + qualified + " | limit 1").pragmas(pragmas))
                ) {
                    assertEquals(getValuesList(ordinaryResponse), getValuesList(sequenceResponse));
                    assertEquals(
                        ordinaryResponse.columns().stream().map(column -> column.type()).toList(),
                        sequenceResponse.columns().stream().map(column -> column.type()).toList()
                    );
                    assertEquals(1, getValuesList(sequenceResponse).size());
                }
            }
        }
    }

    private static Map<String, Object> baseProperties() {
        var properties = new LinkedHashMap<String, Object>();
        properties.put("@timestamp", Map.of("type", "date_nanos"));
        properties.put("key", Map.of("type", "keyword"));
        properties.put("phase", Map.of("type", "keyword"));
        return properties;
    }

    private void createIndex(String index, Map<String, Object> mapping, Settings settings) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put(settings)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.max_docvalue_fields_search", 0)
                )
                .setMapping(mapping)
        );
    }

    private void indexPair(String index, Map<String, Object> values) {
        var bulk = client().prepareBulk();
        for (String phase : List.of("A", "B")) {
            Map<String, Object> source = new LinkedHashMap<>(values);
            source.put("@timestamp", (phase.equals("A") ? BASE : BASE.plusNanos(1)).toString());
            source.put("key", "shared");
            source.put("phase", phase);
            bulk.add(prepareIndex(index).setId(phase).setSource(source));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(index);
    }

    @After
    public void assertSequenceResourcesReleased() throws Exception {
        assertBusy(() -> {
            for (SearchService service : internalCluster().getInstances(SearchService.class)) {
                assertEquals(0, service.getActiveContexts());
            }
        });
        ensureBlocksReleased();
    }
}
