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
import static org.hamcrest.Matchers.notNullValue;

/**
 * Parity tests for {@link DimensionsExtractor} vs the source-parser tsid path
 * ({@link IndexRouting.ExtractFromSource.ForIndexDimensions#buildTsid(XContentType, BytesReference)
 * fed by} {@link XContentParserTsidFunnel}).
 *
 * <p>The extractor must produce a tsid byte-for-byte identical to what the funnel produces, since
 * both feed the same {@link TsidBuilder}. The tsid then determines the shard via
 * {@link IndexRouting.ExtractFromSource#hash(org.apache.lucene.util.BytesRef) hash(tsid)}; equal
 * tsids means equal shard assignments. These tests assert both equalities across primitive types,
 * nested objects, and varying field orders.
 */
public class DimensionsExtractorTests extends ESTestCase {

    public void testParityForFlatStringDimensions() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.host", "node-7");
        doc.put("dim.region", "us-west-2");
        doc.put("metric", "cpu.user");
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityForNestedDimensions() throws IOException {
        Map<String, Object> dim = new LinkedHashMap<>();
        dim.put("host", "node-3");
        dim.put("region", "eu-central-1");
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim", dim);
        doc.put("metric_value", 1.5);
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityForIntegerDimensions() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.shard", 42);
        doc.put("dim.epoch", 1700000000123L);
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityForFloatingPointDimensions() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        // 1.5 narrows to FLOAT in the encoder ((double)(float)1.5 == 1.5); the extractor must still
        // build the same tsid as the funnel which sees DOUBLE.
        doc.put("dim.weight", 1.5);
        // 0.1 cannot be represented exactly as a float, so the encoder keeps DOUBLE.
        doc.put("dim.fraction", 0.1);
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityForBooleanDimensions() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.active", true);
        doc.put("dim.archived", false);
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityWithMixedDimensionTypes() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.name", "alpha");
        doc.put("dim.shard_id", 42);
        doc.put("dim.weight", 1.5);
        doc.put("dim.active", true);
        doc.put("metric", 100);
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testParityWithNullsSkipped() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.host", "h1");
        doc.put("dim.note", null);
        assertExtractorMatchesSourceParser(doc, "dim.*");
    }

    public void testThrowsOnArrayAtDimensionField() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.tags", new String[] { "a", "b", "c" });
        doc.put("metric", "x");
        IndexRouting.ExtractFromSource.ForIndexDimensions strategy = forIndexDimensions("dim.*");
        try (EirfEncoder encoder = new EirfEncoder()) {
            DimensionsExtractor extractor = new DimensionsExtractor(strategy);
            expectThrows(RoutingExtractionException.class, () -> encoder.parseToScratch(toJson(doc), XContentType.JSON, extractor));
        }
    }

    public void testNoThrowOnArrayAtNonDimensionField() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.host", "node-1");
        doc.put("tags", new String[] { "a", "b" });
        IndexRouting.ExtractFromSource.ForIndexDimensions strategy = forIndexDimensions("dim.*");
        try (EirfEncoder encoder = new EirfEncoder()) {
            DimensionsExtractor extractor = new DimensionsExtractor(strategy);
            // Should complete normally — no exception even though the document contains an array.
            encoder.parseToScratch(toJson(doc), XContentType.JSON, extractor);
        }
    }

    public void testTsidIsAttachedToIndexRequest() throws IOException {
        Map<String, Object> doc = new LinkedHashMap<>();
        doc.put("dim.host", "node-7");
        doc.put("dim.region", "us-west-2");

        IndexRouting.ExtractFromSource.ForIndexDimensions strategy = forIndexDimensions("dim.*");
        BytesReference json = toJson(doc);
        IndexRequest req = new IndexRequest("test").id("d1").source(json, XContentType.JSON);
        strategy.preProcess(req);
        try (EirfEncoder encoder = new EirfEncoder()) {
            DimensionsExtractor extractor = new DimensionsExtractor(strategy);
            encoder.parseToScratch(json, XContentType.JSON, extractor);
            extractor.computeShardId(req);
        }
        // After extraction the tsid must be stashed on the request so the data node can reuse it
        // (matches the post-condition of ForIndexDimensions.hashSource).
        assertThat("tsid should be attached to the IndexRequest after extraction", req.tsid(), notNullValue());
    }

    public void testResetClearsBuilderBetweenDocuments() throws IOException {
        IndexRouting.ExtractFromSource.ForIndexDimensions strategyA = forIndexDimensions("dim.*");
        IndexRouting.ExtractFromSource.ForIndexDimensions strategyB = forIndexDimensions("dim.*");

        Map<String, Object> docA = Map.of("dim.host", "alpha");
        Map<String, Object> docB = Map.of("dim.host", "beta");

        try (EirfEncoder shared = new EirfEncoder(); EirfEncoder fresh = new EirfEncoder()) {
            DimensionsExtractor sharedExt = new DimensionsExtractor(strategyA);

            shared.parseToScratch(toJson(docA), XContentType.JSON, sharedExt);
            int sharedShardA = sharedExt.computeShardId(toIndexRequest(docA));
            sharedExt.reset();
            shared.parseToScratch(toJson(docB), XContentType.JSON, sharedExt);
            int sharedShardB = sharedExt.computeShardId(toIndexRequest(docB));

            DimensionsExtractor freshExt = new DimensionsExtractor(strategyB);
            fresh.parseToScratch(toJson(docB), XContentType.JSON, freshExt);
            int freshShardB = freshExt.computeShardId(toIndexRequest(docB));

            assertThat("reset() must isolate documents within a shared extractor", sharedShardB, equalTo(freshShardB));
            assertNotEquals(sharedShardA, sharedShardB);
        }
    }

    /**
     * Encodes the document via the extractor path and via the source-parser path, comparing both
     * the resulting tsid bytes and the resulting shard id.
     */
    private void assertExtractorMatchesSourceParser(Map<String, Object> doc, String dimensionPath) throws IOException {
        IndexRouting.ExtractFromSource.ForIndexDimensions strategySource = forIndexDimensions(dimensionPath);
        IndexRouting.ExtractFromSource.ForIndexDimensions strategyExtractor = forIndexDimensions(dimensionPath);

        BytesReference json = toJson(doc);

        // Source-parser path
        IndexRequest sourceReq = new IndexRequest("test").id("d1").source(json, XContentType.JSON);
        strategySource.preProcess(sourceReq);
        int sourceShardId = strategySource.indexShard(sourceReq);

        // Extractor path
        IndexRequest extractorReq = new IndexRequest("test").id("d1").source(json, XContentType.JSON);
        strategyExtractor.preProcess(extractorReq);
        int extractorShardId;
        try (EirfEncoder encoder = new EirfEncoder()) {
            DimensionsExtractor extractor = new DimensionsExtractor(strategyExtractor);
            encoder.parseToScratch(json, XContentType.JSON, extractor);
            // parseToScratch returning normally implies the extractor didn't throw on any field.
            extractorShardId = extractor.computeShardId(extractorReq);
        }

        assertThat("tsid from extractor must equal tsid from source parser", extractorReq.tsid(), equalTo(sourceReq.tsid()));
        assertThat("shard id parity", extractorShardId, equalTo(sourceShardId));
    }

    private static IndexRouting.ExtractFromSource.ForIndexDimensions forIndexDimensions(String dimensionPath) {
        Settings settings = Settings.builder()
            .put(SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
            .put(IndexMetadata.INDEX_DIMENSIONS.getKey(), dimensionPath)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .build();
        IndexMetadata md = IndexMetadata.builder("test").settings(settings).numberOfShards(8).numberOfReplicas(0).build();
        IndexRouting routing = IndexRouting.fromIndexMetadata(md);
        return (IndexRouting.ExtractFromSource.ForIndexDimensions) routing;
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
