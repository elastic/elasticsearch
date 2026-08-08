/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutComposableIndexTemplateAction;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.esql.action.EsqlPluginWithEnterpriseOrTrialLicense;
import org.elasticsearch.xpack.esql.action.EsqlQueryAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.esql.datasources.datasource.TestEncryptionServicePlugin;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.FieldExtract;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;

public class ColumnarFlattenedFieldIndexingIT extends ESSingleNodeTestCase {

    /**
     * Number of documents to index. Chosen so that the first Lucene flush
     * accumulates &gt; 32 MiB of SortedSlotAccumulator data, triggering the
     * external-sort path in {@code SortedSlotAccumulator}.
     *
     * <p>Each doc contains 1 024 sub-keys in the flattened field
     * (~12 KB of JSON). At a 32 MiB index buffer the flush fires after ~2 730 docs;
     * those 2 730 docs × 1 024 slots × 15 bytes/slot ≈ 42 MiB &gt; 32 MiB
     * → external sort path taken.
     */
    private static final int NUM_DOCS = 3_000;

    /**
     * Total number of distinct sub-key names in the rotating key pool.
     * Each doc draws {@value #ROTATING_KEYS_PER_DOC} keys from this pool.
     */
    private static final int KEY_POOL_SIZE = 10_000;

    /** Rotating keys per doc (stride 10 through the pool ensures all 10 000 keys are used). */
    private static final int ROTATING_KEYS_PER_DOC = 1_023;

    private static final String DATA_STREAM = "logs-test-default";

    private static final String MAPPING = """
        {
          "_doc": {
            "properties": {
              "@timestamp": {"type": "date"},
              "host":       {"properties": {"name": {"type": "keyword"}}},
              "message":    {"type": "keyword"},
              "labels":     {"type": "flattened"}
            }
          }
        }
        """;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(
            InternalSettingsPlugin.class,
            XPackPlugin.class,
            LogsDBPlugin.class,
            DataStreamsPlugin.class,
            TestEncryptionServicePlugin.class,
            EsqlPluginWithEnterpriseOrTrialLicense.class
        );
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put("cluster.columnar.enabled", "true")
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            // Small index buffer so a single flush covers ~2 730 docs and the first
            // flush's accumulator (2730 × 1024 × 15 B ≈ 42 MiB) exceeds this limit,
            // forcing the external-sort path in SortedSlotAccumulator.
            .put("indices.memory.index_buffer_size", ByteSizeValue.ofMb(32).toString())
            .build();
    }

    /**
     * Indexes {@value #NUM_DOCS} documents into a {@code logsdb_columnar} data stream where
     * each document carries 1 024 sub-fields inside a {@code flattened}
     * field. The first Lucene flush accumulates enough SortedSlotAccumulator data to exceed
     * the 32 MiB in-memory threshold, exercising the external-sort code path.
     *
     * <p>After indexing, two ES|QL {@code field_extract} queries are run to verify that
     * the columnar doc-values are readable after an external-sort flush:
     * <ul>
     *   <li>A {@code WHERE} query filtering on an exact sub-key value.</li>
     *   <li>A {@code SORT} query ordering by a sub-key value.</li>
     * </ul>
     */
    public void testIndexingAndQuerying() throws IOException {
        createTemplate();
        bulkIndex();
        client().admin().indices().refresh(new RefreshRequest(DATA_STREAM)).actionGet();

        assumeTrue(
            "field_extract() is snapshot-only; skipping ES|QL assertions on this build",
            FieldExtract.isFnFieldExtractCapabilityMet()
        );

        verifyWhereQuery();
        verifySortQuery();
    }

    // -------------------------------------------------------------------------

    private void createTemplate() throws IOException {
        var putTemplateRequest = new TransportPutComposableIndexTemplateAction.Request("columnar-flattened-test");
        putTemplateRequest.indexTemplate(
            ComposableIndexTemplate.builder()
                .indexPatterns(List.of("logs-test-*"))
                .template(
                    new Template(
                        Settings.builder()
                            .put("index.mode", "logsdb_columnar")
                            .put("index.number_of_shards", 1)
                            .put("index.number_of_replicas", 0)
                            .build(),
                        new CompressedXContent(MAPPING),
                        null
                    )
                )
                .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false))
                .build()
        );
        client().execute(TransportPutComposableIndexTemplateAction.TYPE, putTemplateRequest).actionGet();
    }

    private void bulkIndex() {
        final int batchSize = 500;
        final String baseTimestamp = Instant.now().toString();

        for (int start = 0; start < NUM_DOCS; start += batchSize) {
            BulkRequest bulk = new BulkRequest(DATA_STREAM);
            int end = Math.min(start + batchSize, NUM_DOCS);
            for (int i = start; i < end; i++) {
                bulk.add(
                    new IndexRequest(DATA_STREAM).opType(DocWriteRequest.OpType.CREATE)
                        .source(docSource(i, baseTimestamp), XContentType.JSON)
                );
            }
            BulkResponse response = client().bulk(bulk).actionGet();
            assertFalse("bulk batch [" + start + ".." + end + "] had failures", response.hasFailures());
        }
    }

    /**
     * Builds the JSON source for document {@code docIdx}.
     *
     * <p>The {@code labels} flattened field contains:
     * <ul>
     *   <li>{@code check_key}: {@code "val-<docIdx>"} — unique per document; used for
     *       verifiable WHERE and SORT assertions.</li>
     *   <li>{@value #ROTATING_KEYS_PER_DOC} rotating keys drawn from the
     *       {@value #KEY_POOL_SIZE}-key pool with stride 10, ensuring all pool keys
     *       are covered across the full dataset.</li>
     * </ul>
     */
    private static String docSource(int docIdx, String timestamp) {
        // Rough capacity: "@timestamp"(40) + host(30) + message(20) + labels header(10)
        // + check_key entry(25) + ROTATING_KEYS_PER_DOC × avg 12 chars each
        StringBuilder sb = new StringBuilder(50 + ROTATING_KEYS_PER_DOC * 12);
        sb.append("{\"@timestamp\":\"").append(timestamp).append("\",");
        sb.append("\"host\":{\"name\":\"host-").append(docIdx % 100).append("\"},");
        sb.append("\"message\":\"msg-").append(docIdx).append("\",");
        sb.append("\"labels\":{");
        sb.append("\"check_key\":\"val-").append(docIdx).append("\"");
        // Stride-10 rotation through KEY_POOL_SIZE keys: gcd(10, 10 000) = 10, so
        // 10 000 / 10 = 1 000 distinct starting positions; with NUM_DOCS = 3 000 each
        // starting position repeats 3 times and all 10 000 keys are covered.
        int startKey = (docIdx * 10) % KEY_POOL_SIZE;
        for (int j = 0; j < ROTATING_KEYS_PER_DOC; j++) {
            int keyIdx = (startKey + j) % KEY_POOL_SIZE;
            // Zero-pad to 5 digits so lexicographic order matches numeric order.
            // keyIdx is always in [0, KEY_POOL_SIZE) = [0, 10 000).
            sb.append(",\"k");
            if (keyIdx < 10) sb.append("0000");
            else if (keyIdx < 100) sb.append("000");
            else if (keyIdx < 1000) sb.append("00");
            else sb.append("0");
            sb.append(keyIdx).append("\":\"v\"");
        }
        sb.append("}}");
        return sb.toString();
    }

    /**
     * Runs {@code field_extract(labels, "check_key") == "val-0"} and asserts exactly one
     * document matches (document 0).
     */
    private void verifyWhereQuery() {
        String query = "FROM "
            + DATA_STREAM
            + " | EVAL v = field_extract(labels, \"check_key\")"
            + " | WHERE v == \"val-0\""
            + " | KEEP v"
            + " | LIMIT 10";
        try (EsqlQueryResponse response = esql(query)) {
            List<List<Object>> rows = valuesList(response);
            assertThat("WHERE field_extract(labels, 'check_key') == 'val-0' should match exactly one doc", rows, hasSize(1));
            assertThat(stringValue(rows.getFirst().getFirst()), equalTo("val-0"));
        }
    }

    /**
     * Runs a SORT on {@code field_extract(labels, "check_key")} and verifies the first
     * five results are in ascending lexicographic order.
     *
     * <p>The values are {@code "val-0", "val-1", ..., "val-2999"}, so lexicographic order
     * gives {@code val-0, val-1, val-10, val-100, val-1000} as the first five.
     */
    private void verifySortQuery() {
        String query = "FROM "
            + DATA_STREAM
            + " | EVAL v = field_extract(labels, \"check_key\")"
            + " | WHERE v IS NOT NULL"
            + " | SORT v ASC"
            + " | KEEP v"
            + " | LIMIT 5";
        try (EsqlQueryResponse response = esql(query)) {
            List<List<Object>> rows = valuesList(response);
            assertThat("SORT on field_extract should return at least 5 rows", rows.size(), greaterThan(4));
            // Lexicographic order of "val-N" for N in 0..2999:
            // val-0 < val-1 < val-10 < val-100 < val-1000
            assertThat(stringValue(rows.get(0).getFirst()), equalTo("val-0"));
            assertThat(stringValue(rows.get(1).getFirst()), equalTo("val-1"));
            assertThat(stringValue(rows.get(2).getFirst()), equalTo("val-10"));
            assertThat(stringValue(rows.get(3).getFirst()), equalTo("val-100"));
            assertThat(stringValue(rows.get(4).getFirst()), equalTo("val-1000"));
        }
    }

    private EsqlQueryResponse esql(String query) {
        return client().execute(EsqlQueryAction.INSTANCE, EsqlQueryRequest.syncEsqlQueryRequest(query)).actionGet();
    }

    private static List<List<Object>> valuesList(EsqlQueryResponse response) {
        List<List<Object>> result = new ArrayList<>();
        Iterator<Iterator<Object>> rows = response.values();
        while (rows.hasNext()) {
            List<Object> row = new ArrayList<>();
            Iterator<Object> cols = rows.next();
            while (cols.hasNext()) {
                row.add(cols.next());
            }
            result.add(row);
        }
        return result;
    }

    /** Converts a value from the ESQL response to a String regardless of whether it is a {@link BytesRef} or already a String. */
    private static String stringValue(Object v) {
        if (v instanceof BytesRef br) {
            return br.utf8ToString();
        }
        return v.toString();
    }
}
