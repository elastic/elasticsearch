/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.heap_attack;

import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.matchesMap;

/**
 * Heap-attack coverage for the binary doc-values decode buffer that a Painless script opens when it reads
 * {@code doc['<flattened key>']}.
 *
 * <p>Under the time-series doc-values format a {@code flattened} field stores its keyed values in binary doc
 * values. Each binary doc-values reader lazily allocates one {@code uncompressedBlock} buffer, sized up to the
 * large-binary-block cap of 512 KB, on the first block it decodes. A runtime-field query that reads a flattened
 * key opens such a reader for every leaf: each {@code AbstractScriptFieldQuery} builds its own
 * {@code LeafSearchLookup -> LeafDocLookup -> FieldFactoryWrapper -> FlattenedDocValuesField ->
 * KeyedFlattenedBinaryDocValues -> BinaryDecoder} per leaf, so the reader is not shared across clauses. A
 * {@code filters} aggregation with many runtime-field clauses therefore holds roughly {@code (clauses) x (leaves)}
 * of these 512 KB buffers at once, and in #3404 none of them was accounted for in a circuit breaker, so the search
 * node exhausted its heap instead of rejecting the request.
 *
 * <p>The earlier buffer-accounting fixes do not cover this entry point: #153903 only helps readers that never
 * decode a block (every script here decodes at least one), and #160584 accounts clause cost only for the
 * {@code Scanning*BinaryDocValues*} query markers, not for a reader opened at run time through the doc lookup.
 *
 * <p>This test guards the fix: {@code LeafDocLookup} accounts each newly opened reader's
 * {@code maxDecodeBytes()} estimate on the request breaker (label {@code script_doc_values:<field>}), released when
 * the leaf it was opened against is done being collected. The trip still happens because all clause readers of a
 * single leaf are alive — and accounted — at once. The breaker trips inside the Painless script, so the failure surfaces wrapped in a
 * {@code script_exception} inside a {@code search_phase_execution_exception}, with status 429 because
 * {@code ScriptException#status()} recognizes a circuit-breaker cause.
 */
public class HeapAttackScriptBinaryDocValuesIT extends HeapAttackTestCase {
    private static final String FLATTENED_INDEX = "flattened_script_source";

    /**
     * Index:
     * <ul>
     *     <li>Synthetic source</li>
     *     <li>Time-series doc-values format, so the {@code flattened} field is stored in binary doc values</li>
     *     <li>A {@code flattened} field {@code app} whose {@code request.uri} key holds ~2 KB values, so binary
     *         blocks fill to the 512 KB large-binary-block cap and every decode buffer reaches full size</li>
     *     <li>Several shards, so there are several leaves to open readers against</li>
     * </ul>
     * Query:
     * <ul>
     *     <li>A {@code filters} aggregation whose clauses are {@code exists} queries over distinct keyword runtime
     *         fields, each defined by a Painless script that reads {@code doc['app.request.uri']}</li>
     * </ul>
     * Expected: Circuit break, rather than a heap-exhausting pile of uncounted 512 KB decode buffers.
     */
    public void testFiltersAggRuntimeFieldsOnFlattenedField() throws IOException {
        initFlattenedIndex(2000, 3, Math.toIntExact(ByteSizeValue.ofKb(2).getBytes()));

        try {
            setRequestBreakerLimit("40%");
            // Each runtime-field clause opens one 512 KB binary doc-values decode buffer per leaf. ~400 clauses on
            // the first attempt is already ~200 MB of buffers per leaf; later attempts scale past the breaker limit.
            // The breaker trips while a Painless script loads doc values, so the search fails with a
            // script_exception wrapping the circuit_breaking_exception, and the whole request surfaces as a
            // search_phase_execution_exception that keeps the breaker's 429 status.
            assertCircuitBreaks(
                attempt -> fetchWithRuntimeFieldFilters(attempt * 400),
                matchesMap().entry("status", 429).entry("error", matchesMap().extraOk().entry("type", "search_phase_execution_exception"))
            );
        } finally {
            setRequestBreakerLimit(null);
        }
    }

    private void initFlattenedIndex(int docs, int shards, int uriBytes) throws IOException {
        logger.info("loading {} documents with a flattened field whose request.uri key holds {} byte values", docs, uriBytes);
        String mapping = "{\"properties\": {\"sort_key\": {\"type\": \"long\"}, \"app\": {\"type\": \"flattened\"}}}";
        CreateIndexResponse response = createIndex(
            FLATTENED_INDEX,
            Settings.builder()
                .put("index.number_of_shards", shards)
                .put("index.mapping.source.mode", "synthetic")
                .put("index.use_time_series_doc_values_format", true)
                // Runtime fields in the request count against this limit; keep plenty of headroom for the fan-out.
                .put("index.mapping.total_fields.limit", 20000)
                .build(),
            mapping
        );
        assertTrue(response.isAcknowledged());

        // A URI-shaped value: a fixed prefix plus filler, long enough that ~256 docs fill a 512 KB binary block.
        String uri = "/api/" + "a".repeat(Math.max(0, uriBytes - 5));
        int docsPerBulk = 100;
        StringBuilder bulk = new StringBuilder();
        for (int d = 0; d < docs; d++) {
            bulk.append("{\"create\":{}}\n");
            bulk.append("{\"sort_key\":").append(d);
            bulk.append(",\"app\":{\"request\":{\"uri\":\"").append(uri).append("\"}}}\n");
            if (d % docsPerBulk == docsPerBulk - 1 && d != docs - 1) {
                bulk(FLATTENED_INDEX, bulk.toString());
                bulk.setLength(0);
            }
        }
        initIndex(FLATTENED_INDEX, bulk.toString());
    }

    /**
     * Runs a {@code filters} aggregation with {@code filters} clauses, each an {@code exists} query over its own
     * keyword runtime field. Every runtime field uses the same script source (compiled once), reading
     * {@code doc['app.request.uri']}, but because each field yields a distinct {@code AbstractScriptFieldQuery} the
     * readers are not shared: every clause opens its own binary doc-values decode buffer per leaf.
     */
    private Map<String, Object> fetchWithRuntimeFieldFilters(int filters) throws IOException {
        logger.info("running a filters aggregation with {} runtime-field exists clauses reading doc['app.request.uri']", filters);
        String source = "if (doc['app.request.uri'].size() > 0) { emit(doc['app.request.uri'].value); }";
        StringBuilder body = new StringBuilder();
        body.append("{\"size\":0,\"track_total_hits\":false,\"runtime_mappings\":{");
        for (int f = 0; f < filters; f++) {
            if (f > 0) {
                body.append(',');
            }
            body.append("\"r").append(f).append("\":{\"type\":\"keyword\",\"script\":{\"source\":\"").append(source).append("\"}}");
        }
        body.append("},\"aggs\":{\"f\":{\"filters\":{\"filters\":{");
        for (int f = 0; f < filters; f++) {
            if (f > 0) {
                body.append(',');
            }
            body.append('"').append(f).append("\":{\"exists\":{\"field\":\"r").append(f).append("\"}}");
        }
        // Close the clause map and the "filters" config, then give the filters agg a sub-aggregation. A child agg
        // forces the "compatible" MultiFilterLeafCollector path, which builds a scorer for every clause up front
        // (all per-leaf readers and their decode buffers live at once) rather than the fast one-clause-at-a-time path.
        body.append("}},\"aggs\":{\"c\":{\"value_count\":{\"field\":\"sort_key\"}}}");
        body.append("}}}");

        Request request = new Request("POST", "/" + FLATTENED_INDEX + "/_search");
        request.addParameter("error_trace", "");
        request.addParameter("request_cache", "false");
        // A single shard tripping its breaker must fail the request; otherwise a partial 200 would force another,
        // heavier attempt.
        request.addParameter("allow_partial_search_results", "false");
        request.setJsonEntity(body.toString());
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder()
                .setRequestConfig(RequestConfig.custom().setSocketTimeout(Math.toIntExact(TimeValue.timeValueMinutes(6).millis())).build())
        );
        return responseAsMap(runQuery(() -> client().performRequest(request)));
    }
}
