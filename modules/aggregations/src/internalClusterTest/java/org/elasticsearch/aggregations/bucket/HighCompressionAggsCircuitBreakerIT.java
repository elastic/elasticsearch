/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.aggregations.bucket;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.aggregations.AggregationIntegTestCase;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING;
import static org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.elasticsearch.search.aggregations.AggregationBuilders.composite;
import static org.elasticsearch.search.aggregations.AggregationBuilders.topHits;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Verifies that the {@code REQUEST} circuit breaker trips during aggregation reduction when the on-the-wire
 * (compressed) representation of the aggs is much smaller than the in-memory (uncompressed) form.
 * <p>
 * Background: since #126988, {@link org.elasticsearch.common.io.stream.DelayableWriteable} compresses its
 * payload with a typical 10–16× compression ratio for aggregations. Pre-fix
 * (<a href="https://github.com/elastic/elasticsearch/issues/147190">#147190</a>) the consumer accounted only
 * for the compressed bytes, so a workload whose <em>uncompressed</em> aggregation tree exceeded the breaker
 * limit would silently sail through. This test feeds the cluster a highly compressible payload
 * (long repeated strings) and asserts that the breaker now fires.
 * <p>
 * The previous, more sensitive {@code AggregationReductionCircuitBreakingIT} was muted as flaky in
 * <a href="https://github.com/elastic/elasticsearch/issues/134667">#134667</a> precisely because it relied
 * on numeric payloads (poor compression) and a tight margin against the 7&nbsp;MB breaker. This rewrite
 * uses a workload where the compressed/uncompressed gap is large enough that pre-fix the accounting
 * under-counted by ~10×, making the post-fix assertion deterministic.
 */
@ESIntegTestCase.ClusterScope(minNumDataNodes = 1, maxNumDataNodes = 2, numClientNodes = 1)
public class HighCompressionAggsCircuitBreakerIT extends AggregationIntegTestCase {

    private static final String INDEX = "compressible_aggs";
    private static final int NUM_FIELDS = 8;
    private static final int FIELD_LENGTH = 4_000;
    private static final int DOC_COUNT = 200;
    private static final int BUCKET_KEYS = 25; // docs per bucket = DOC_COUNT / BUCKET_KEYS = 8

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        var settings = Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            // single search thread for determinism — matches the prior IT (#134667).
            .put("thread_pool.search.size", 1);
        if (NODE_ROLES_SETTING.get(otherSettings).isEmpty()) {
            // Coordinator: tight breaker. The uncompressed aggregation tree should easily exceed this; the
            // compressed wire representation does not.
            settings.put(REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "7MB");
        } else {
            // Data nodes: keep memory usage real so we don't OOM during indexing/reduce.
            settings.put(USE_REAL_MEMORY_USAGE_SETTING.getKey(), true).put(TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "80%");
        }
        return settings.build();
    }

    public void testBreakerTripsOnHighlyCompressibleReduction() throws IOException {
        createIndex();
        indexDocs();

        List<CompositeValuesSourceBuilder<?>> sources = new ArrayList<>();
        sources.add(new TermsValuesSourceBuilder("k").field("bucket_key"));

        var search = internalCluster().coordOnlyNodeClient()
            .prepareSearch(INDEX)
            .setSize(0)
            .addAggregation(composite("by_key", sources).size(5_000).subAggregation(topHits("hits").size(10)))
            // Force partial reductions to run during the query phase.
            .setBatchedReduceSize(randomIntBetween(2, 5));

        try {
            search.get().decRef();
            fail("expected REQUEST circuit breaker to trip while reducing aggregations");
        } catch (SearchPhaseExecutionException e) {
            String fullStack = ExceptionsHelper.stackTrace(e);
            // If a shard failed (e.g. due to data-node memory pressure) we can't make claims about the coordinator-side reduction.
            assumeTrue(fullStack, e.shardFailures().length == 0);
            assertThat(e.getCause(), instanceOf(CircuitBreakingException.class));
            assertThat(fullStack, containsString("[request] Data too large"));
            assertThat(fullStack, containsString("org.elasticsearch.action.search.QueryPhaseResultConsumer"));
        }
    }

    private void createIndex() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping.startObject("bucket_key").field("type", "keyword").endObject();
        for (int i = 0; i < NUM_FIELDS; i++) {
            mapping.startObject("payload_" + i).field("type", "keyword").endObject();
        }
        mapping.endObject().endObject();

        assertAcked(
            prepareCreate(INDEX).setSettings(Settings.builder().put("index.number_of_shards", randomIntBetween(2, 5)).build())
                .setMapping(mapping)
        );
        ensureGreen(INDEX);
    }

    private void indexDocs() throws IOException {
        // A long repeated string compresses ~16× under DEFLATE (the algorithm DelayableWriteable uses), so the
        // on-the-wire bytes for the resulting aggregation tree (which echoes these values back inside top_hits
        // source) are far smaller than the expanded heap form.
        final String alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        final String repeated = alphabet.repeat(FIELD_LENGTH / alphabet.length());

        // Sanity-check the test expected compression
        BytesArray uncompressed = new BytesArray(repeated.getBytes(StandardCharsets.UTF_8));
        long compressedLen = CompressorFactory.COMPRESSOR.compress(uncompressed).length();
        assertThat(
            "payload should compress at least 10× to keep the breaker headroom meaningful",
            (double) uncompressed.length() / compressedLen,
            greaterThan(10.0)
        );

        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < DOC_COUNT; i++) {
            XContentBuilder doc = XContentFactory.jsonBuilder().startObject().field("bucket_key", "key_" + (i % BUCKET_KEYS));
            for (int j = 0; j < NUM_FIELDS; j++) {
                doc.field("payload_" + j, repeated);
            }
            doc.endObject();
            docs.add(prepareIndex(INDEX).setOpType(DocWriteRequest.OpType.CREATE).setSource(doc));
        }
        indexRandom(true, false, false, false, docs);
    }
}
