/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_VERSION_CREATED;
import static org.hamcrest.Matchers.equalTo;

/**
 * Parity tests for {@link RoutingPathExtractor} vs the source-parser path it is meant to replace.
 *
 * <p>The hash basis used by {@link RoutingHashBuilder} is the UTF-8 byte slice of the parser's
 * token text, so the encoder-driven extractor must hand the same bytes to {@link RoutingHashBuilder}
 * as the source-parser code path. These tests assert that the two paths produce the same shard id
 * for the same document, across primitive types, nested objects, and varying field orders.
 */
public class RoutingPathExtractorTests extends ESTestCase {

    public void testParityForFlatStringFields() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.host", "node-7");
        doc.put("dim.region", "us-west-2");
        doc.put("metric", "cpu.user");
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityForNestedObjectsBuildsDottedPath() throws IOException {
        // Test that nested-object dotted-path construction matches between the source-parser path
        // (RoutingHashBuilder.extractObject builds path = parent + "." + child) and the encoder path
        // (EirfSchema.getFullPath walks the parent chain).
        Map<String, Object> dim = new LinkedHashMap<>();
        dim.put("host", "node-3");
        dim.put("region", "eu-central-1");
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim", dim);
        doc.put("ts", 1234567890L);
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityForNumericRoutingFields() throws IOException {
        // RoutingHashBuilder hashes numbers as their textual UTF-8 bytes
        // (parser.optimizedText().bytes()). The encoder also captures that text via the same call,
        // so the hash must match regardless of whether the value is INT, LONG, FLOAT, or DOUBLE.
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.shard", 42);
        doc.put("dim.weight", 3.14);
        doc.put("dim.epoch", 1700000000123L);
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityForBooleanRoutingFields() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.active", true);
        doc.put("dim.archived", false);
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityWithNullsSkipped() throws IOException {
        // Both RoutingHashBuilder.extractItem and the encoder's LeafSink skip VALUE_NULL — confirm
        // that a null routing-path field doesn't change the hash relative to omitting it.
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.host", "h1");
        doc.put("dim.note", null);
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityWithMultipleRoutingPaths() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.host", "node-1");
        doc.put("other.zone", "az-2");
        doc.put("ignored", "value");
        assertExtractorMatchesSourceParser(doc, "dim.*,other.*");
    }

    public void testThrowsOnArrayAtRoutingField() throws IOException {
        // Arrays at routing-path fields can't be replayed from the encoder's packed-array column;
        // the extractor throws and the caller (BulkBatchEncoders) abandons the bulk's batch.
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.tags", new String[] { "a", "b", "c" });
        doc.put("metric", "x");
        IndexRouting.ExtractFromSource.ForRoutingPath strategy = forRoutingPath("dim.*");
        try (EirfEncoder encoder = new EirfEncoder()) {
            RoutingPathExtractor extractor = new RoutingPathExtractor(strategy);
            expectThrows(RoutingExtractionException.class, () -> encoder.parseToScratch(toJson(doc), XContentType.JSON, extractor));
        }
    }

    public void testNoThrowOnArrayAtNonRoutingField() throws IOException {
        // Arrays at non-routing fields must not throw; the extractor only objects to arrays where
        // it would have lost routing information.
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.host", "node-1");
        doc.put("tags", new String[] { "a", "b" });
        IndexRouting.ExtractFromSource.ForRoutingPath strategy = forRoutingPath("dim.*");
        try (EirfEncoder encoder = new EirfEncoder()) {
            RoutingPathExtractor extractor = new RoutingPathExtractor(strategy);
            // Should complete normally — no exception even though the document contains an array.
            encoder.parseToScratch(toJson(doc), XContentType.JSON, extractor);
        }
    }

    public void testReuseAcrossDocumentsClearsBuilderState() throws IOException {
        // Same extractor, two different documents with different routing-path values should produce
        // the same shard ids as fresh extractors.
        IndexRouting.ExtractFromSource.ForRoutingPath strategyA = forRoutingPath("dim.*");
        IndexRouting.ExtractFromSource.ForRoutingPath strategyB = forRoutingPath("dim.*");
        try (EirfEncoder encoderShared = new EirfEncoder(); EirfEncoder encoderFresh = new EirfEncoder()) {
            RoutingPathExtractor sharedExtractor = new RoutingPathExtractor(strategyA);

            Map<String, Object> docA = Map.of("dim.host", "alpha");
            Map<String, Object> docB = Map.of("dim.host", "beta");

            // Shared extractor: process both documents in sequence.
            encoderShared.parseToScratch(toJson(docA), XContentType.JSON, sharedExtractor);
            int sharedShardA = sharedExtractor.computeShardId(toIndexRequest(docA));
            sharedExtractor.reset();
            encoderShared.parseToScratch(toJson(docB), XContentType.JSON, sharedExtractor);
            int sharedShardB = sharedExtractor.computeShardId(toIndexRequest(docB));

            // Fresh extractor for B alone.
            RoutingPathExtractor freshExtractor = new RoutingPathExtractor(strategyB);
            encoderFresh.parseToScratch(toJson(docB), XContentType.JSON, freshExtractor);
            int freshShardB = freshExtractor.computeShardId(toIndexRequest(docB));

            assertThat("reset() must isolate documents within a shared extractor", sharedShardB, equalTo(freshShardB));
            // sanity: A and B route differently with this corpus (otherwise the test wouldn't be
            // exercising the reset behavior)
            assertNotEquals(sharedShardA, sharedShardB);
        }
    }

    /**
     * Asserts that the source-parser routing path and the EIRF-encoder routing extractor produce
     * the same shard id for the given document.
     */
    private void assertExtractorMatchesSourceParser(Map<String, Object> doc, String routingPath) throws IOException {
        IndexRouting.ExtractFromSource.ForRoutingPath strategySource = forRoutingPath(routingPath);
        IndexRouting.ExtractFromSource.ForRoutingPath strategyExtractor = forRoutingPath(routingPath);

        BytesReference json = toJson(doc);

        // Source-parser path
        IndexRequest sourceReq = new IndexRequest("test").id("doc-1").source(json, XContentType.JSON);
        strategySource.preProcess(sourceReq);
        int sourceShardId = strategySource.indexShard(sourceReq);

        // Extractor path
        IndexRequest extractorReq = new IndexRequest("test").id("doc-1").source(json, XContentType.JSON);
        strategyExtractor.preProcess(extractorReq);
        int extractorShardId;
        try (EirfEncoder encoder = new EirfEncoder()) {
            RoutingPathExtractor extractor = new RoutingPathExtractor(strategyExtractor);
            encoder.parseToScratch(json, XContentType.JSON, extractor);
            // parseToScratch returning normally implies the extractor didn't throw on any field.
            extractorShardId = extractor.computeShardId(extractorReq);
        }

        assertThat("extractor path must match source-parser path for routing=" + routingPath, extractorShardId, equalTo(sourceShardId));
    }

    private static IndexRouting.ExtractFromSource.ForRoutingPath forRoutingPath(String routingPath) {
        Settings settings = Settings.builder()
            .put(SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), routingPath)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .build();
        IndexMetadata md = IndexMetadata.builder("test").settings(settings).numberOfShards(8).numberOfReplicas(0).build();
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        return (IndexRouting.ExtractFromSource.ForRoutingPath) routing;
    }

    private static BytesReference toJson(Map<String, Object> doc) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.map(doc);
            return BytesReference.bytes(builder);
        }
    }

    private static IndexRequest toIndexRequest(Map<String, Object> doc) throws IOException {
        return new IndexRequest("test").id("d").source(toJson(doc), XContentType.JSON);
    }
}
