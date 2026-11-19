/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.operator.OperatorStatus;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.esql.action.AbstractSequenceIntegTestCase;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.junit.After;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class SequenceKeyFilterLogsdbIT extends AbstractSequenceIntegTestCase {
    private static final Instant START = Instant.parse("2026-11-17T00:00:00Z");
    private static final String INDEXED_KEYWORD = "{\"type\":\"keyword\",\"index\":true}";

    public void testIndexedKeywordKey() {
        String index = "sequence-logsdb-keyword";
        var type = (KeywordFieldMapper.KeywordFieldType) create(index, IndexMode.LOGSDB, "key", INDEXED_KEYWORD, Settings.EMPTY);
        assertTrue(type.indexType().hasTerms());
        assertFalse(type.usesBinaryDocValues());
        assertNull(SequenceSourceQueryBuilder.keywordTermRejection(type, "wanted"));
        seed(index, "key", "wanted", "unrelated", "keyword");
        assertQuery(index, "key", List.of(row("wanted", "keyword")), 3, 2, profile(1L, Map.of(), Map.of("logsdb", 1L)));
    }

    public void testIndexedNumericKey() {
        String index = "sequence-logsdb-integer";
        var type = create(index, IndexMode.LOGSDB, "key", "{\"type\":\"integer\",\"index\":true}", Settings.EMPTY);
        assertTrue(type.indexType().hasPoints());
        seed(index, "key", 42, 99, "integer");
        assertQuery(index, "key", List.of(row(42, "integer")), 3, 2, profile(1L, Map.of(), Map.of("logsdb", 1L)));
    }

    public void testIndexedBooleanKey() {
        String index = "sequence-logsdb-boolean";
        var type = create(index, IndexMode.LOGSDB, "key", "{\"type\":\"boolean\",\"index\":true}", Settings.EMPTY);
        assertTrue(type.indexType().hasTerms());
        seed(index, "key", true, false, "boolean");
        assertQuery(index, "key", List.of(row(true, "boolean")), 3, 2, profile(1L, Map.of(), Map.of("logsdb", 1L)));
    }

    public void testHostnameSkipperFallsBack() {
        String index = "sequence-logsdb-hostname";
        Settings settings = Settings.builder()
            .put(IndexSettings.USE_DOC_VALUES_SKIPPER.getKey(), true)
            .put("index.sort.field", "host.name")
            .build();
        var type = create(index, IndexMode.LOGSDB, "host.name", "{\"type\":\"keyword\"}", settings);
        assertTrue(type.indexType().hasDocValuesSkipper());
        assertFalse(type.indexType().hasTerms());
        seed(index, "host.name", "wanted", "unrelated", "hostname");
        assertQuery(
            index,
            "host.name",
            List.of(row("wanted", "hostname")),
            3,
            3,
            profile(0L, Map.of("no_indexed_terms", 1L), Map.of("logsdb", 1L))
        );
    }

    public void testBinaryKeywordInLogsdbColumnarFallsBack() {
        String index = "sequence-logsdb-binary";
        // The mapping exposes binary keyword doc values only on strict columnar modes. The mode guard rejects
        // that index before a search can reach its keyword guard, so exercise both decisions on the same real mapper.
        var type = (KeywordFieldMapper.KeywordFieldType) create(index, IndexMode.LOGSDB_COLUMNAR, "key", INDEXED_KEYWORD, Settings.EMPTY);
        assertTrue(type.usesBinaryDocValues());
        assertTrue(type.hasDocValues());
        assertEquals("binary_doc_values", SequenceSourceQueryBuilder.keywordTermRejection(type, "wanted"));
        seed(index, "key", "wanted", "unrelated", "binary");
        assertQuery(
            index,
            "key",
            List.of(row("wanted", "binary")),
            3,
            3,
            profile(0L, Map.of("index_mode", 1L), Map.of("logsdb_columnar", 1L))
        );
    }

    public void testMixedLogsdbMappingsApplyPerShard() {
        String prefix = "sequence-logsdb-mixed-";
        String eligible = prefix + "eligible";
        String ineligible = prefix + "ineligible";
        var indexed = create(eligible, IndexMode.LOGSDB, "key", INDEXED_KEYWORD, Settings.EMPTY);
        var docValuesOnly = create(ineligible, IndexMode.LOGSDB, "key", "{\"type\":\"keyword\",\"index\":false}", Settings.EMPTY);
        assertTrue(indexed.indexType().hasTerms());
        assertFalse(docValuesOnly.indexType().hasTerms());
        assertTrue(docValuesOnly.hasDocValues());
        seed(eligible, "key", "wanted-one", "unrelated-one", "eligible");
        seed(ineligible, "key", "wanted-two", "unrelated-two", "ineligible");
        assertQuery(
            prefix + "*",
            "key",
            List.of(row("wanted-one", "eligible"), row("wanted-two", "ineligible")),
            6,
            5,
            profile(1L, Map.of("no_indexed_terms", 1L), Map.of("logsdb", 2L))
        );
    }

    private MappedFieldType create(String index, IndexMode mode, String key, String mapping, Settings additional) {
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_shards", 1)
                        .put("index.number_of_replicas", 0)
                        .put(IndexSettings.MODE.getKey(), mode.getName())
                        .put(additional)
                )
                .setMapping("""
                    {"dynamic":false,"properties":{
                      "@timestamp":{"type":"date_nanos","index":true},
                      "phase":{"type":"keyword","index":true},
                      "payload":{"type":"keyword"},
                      "%s":%s
                    }}
                    """.formatted(key, mapping))
        );
        ensureGreen(index);
        var service = internalCluster().getDataNodeInstance(IndicesService.class).indexService(resolveIndex(index));
        assertNotNull(service);
        assertEquals(mode, service.getIndexSettings().getMode());
        return service.mapperService().fieldType(key);
    }

    private void seed(String index, String key, Object waiting, Object outside, String suffix) {
        assertFalse(
            client().prepareBulk()
                .add(event(index, key, "A-" + suffix, waiting, "A", 0))
                .add(event(index, key, "noise-" + suffix, outside, "B", 1))
                .add(event(index, key, "B-" + suffix, waiting, "B", 2))
                .get()
                .hasFailures()
        );
        refresh(index);
    }

    private IndexRequestBuilder event(String index, String key, String id, Object value, String phase, int millis) {
        return prepareIndex(index).setId(id)
            .setSource(Map.of("@timestamp", START.plusMillis(millis).toString(), key, value, "phase", phase, "payload", "payload-" + id));
    }

    private static List<Object> row(Object key, String suffix) {
        return List.of(key, "A-" + suffix, "B-" + suffix, "payload-B-" + suffix);
    }

    private static Map<String, Object> profile(long applied, Map<String, Long> rejected, Map<String, Long> indexModes) {
        return Map.of(
            "built_searches",
            1L,
            "applied_shard_searches",
            applied,
            "coordinator_fallbacks",
            Map.of(),
            "rejected_shard_searches",
            rejected,
            "index_modes",
            indexModes
        );
    }

    private void assertQuery(
        String index,
        String key,
        List<List<Object>> expectedRows,
        long offFetched,
        long onFetched,
        Map<String, Object> expectedProfile
    ) {
        String query = """
            SEQUENCE BY %s WITHIN 1m PRIMARY 1
            (FROM %s METADATA _id | WHERE phase=="A"),(FROM %s METADATA _id | WHERE phase=="B")
            | EVAL second_id=MEMBER(2,_id),second_payload=MEMBER(2,payload)
            | KEEP %s,_id,second_id,second_payload | SORT _id | LIMIT 10
            """.formatted(key, index, index, key);
        Observed off = observe(query, false);
        Observed on = observe(query, true);
        assertEquals(expectedRows, off.rows());
        assertEquals(off.rows(), on.rows());
        assertEquals(offFetched, off.status().candidateRowsFetched());
        assertEquals(onFetched, on.status().candidateRowsFetched());
        assertNull(off.keyFilterProfile());
        assertEquals(expectedProfile, on.keyFilterProfile());
        logger.info(
            "key-filter logsdb index={} key={} off_fetched={} on_fetched={} profile={}",
            index,
            key,
            off.status().candidateRowsFetched(),
            on.status().candidateRowsFetched(),
            on.keyFilterProfile()
        );
    }

    private Observed observe(String query, boolean enabled) {
        var pragmas = new QueryPragmas(
            Settings.builder().put("sequence_key_filter", enabled).put("sequence_key_filter_shard_profile", true).build()
        );
        try (var response = run(syncEsqlQueryRequest(query).allowPartialResults(false).profile(true).pragmas(pragmas))) {
            assertFalse(response.isPartial());
            assertNotNull(response.profile());
            var statuses = response.profile()
                .drivers()
                .stream()
                .flatMap(driver -> driver.operators().stream())
                .map(OperatorStatus::status)
                .filter(SequenceSourceStatus.class::isInstance)
                .map(SequenceSourceStatus.class::cast)
                .toList();
            assertEquals(1, statuses.size());
            var keyFilterProfile = response.getExecutionInfo().queryProfile().sequenceKeyFilterSnapshot();
            return new Observed(getValuesList(response), statuses.getFirst(), keyFilterProfile);
        }
    }

    private record Observed(List<List<Object>> rows, SequenceSourceStatus status, Map<String, Object> keyFilterProfile) {}

    @After
    public void assertReleased() throws Exception {
        for (SearchService service : internalCluster().getInstances(SearchService.class)) {
            assertBusy(() -> assertEquals(0, service.getActiveContexts()));
        }
        ensureBlocksReleased();
    }
}
