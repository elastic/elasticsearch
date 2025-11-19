/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.diversification;

import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.MockResolvedIndices;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperBuilderContext;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.RootObjectMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.search.vectors.VectorData;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;

public class DiversifyRetrieverBuilderTests extends ESTestCase {

    public void testValidate() {
        SearchSourceBuilder source = new SearchSourceBuilder();

        var retrieverWithLargeSize = new DiversifyRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "test_field",
            10,
            20,
            getRandomQueryVector(),
            0.3f
        );
        var validationSize = retrieverWithLargeSize.validate(source, null, false, false);
        assertEquals(1, validationSize.validationErrors().size());
        assertEquals(
            "[diversify] MMR result diversification [size] of 20 cannot be greater than the [rank_window_size] of 10",
            validationSize.validationErrors().getFirst()
        );

        int rankWindowSize = randomIntBetween(1, 20);
        Integer size = randomBoolean() ? null : randomIntBetween(1, rankWindowSize);

        // ensure lambda is within range and set
        var retrieverHighLambda = new DiversifyRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "test_field",
            rankWindowSize,
            size,
            getRandomQueryVector(),
            2.0f
        );
        var validationLambda = retrieverHighLambda.validate(source, null, false, false);
        assertEquals(1, validationLambda.validationErrors().size());
        assertEquals(
            "[diversify] MMR result diversification must have a [lambda] between 0.0 and 1.0. The value provided was 2.0",
            validationLambda.validationErrors().getFirst()
        );

        var retrieverLowLambda = new DiversifyRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "test_field",
            rankWindowSize,
            size,
            getRandomQueryVector(),
            -0.1f
        );
        validationLambda = retrieverLowLambda.validate(source, null, false, false);
        assertEquals(1, validationLambda.validationErrors().size());
        assertEquals(
            "[diversify] MMR result diversification must have a [lambda] between 0.0 and 1.0. The value provided was -0.1",
            validationLambda.validationErrors().getFirst()
        );

        var retrieverNullLambda = new DiversifyRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "test_field",
            rankWindowSize,
            size,
            getRandomQueryVector(),
            null
        );
        validationLambda = retrieverNullLambda.validate(source, null, false, false);
        assertEquals(1, validationLambda.validationErrors().size());
        assertEquals(
            "[diversify] MMR result diversification must have a [lambda] between 0.0 and 1.0. The value provided was null",
            validationLambda.validationErrors().getFirst()
        );
    }

    public void testClone() {
        var original = createRandomRetriever();
        var clonedWithSameRetriever = original.clone(List.of(original.innerRetrievers().getFirst()), null);
        assertNotSame(original, clonedWithSameRetriever);
        assertTrue(original.doEquals(clonedWithSameRetriever));

        CompoundRetrieverBuilder.RetrieverSource newInnerRetriever = getInnerRetriever();
        var cloned = original.clone(List.of(newInnerRetriever), null);
        assertNotSame(original, cloned);
        assertFalse(original.doEquals(cloned));

        // make sure we have to have one and only one new inner retriever
        AssertionError exNoRetrievers = Assert.assertThrows(AssertionError.class, () -> original.clone(List.of(), null));
        assertEquals("ResultDiversificationRetrieverBuilder must have a single child retriever", exNoRetrievers.getMessage());

        AssertionError exTooMany = Assert.assertThrows(
            AssertionError.class,
            () -> original.clone(List.of(newInnerRetriever, newInnerRetriever), null)
        );
        assertEquals("ResultDiversificationRetrieverBuilder must have a single child retriever", exTooMany.getMessage());
    }

    public void testDoRewrite() {
        var queryRewriteContext = getQueryRewriteContext();
        var original = createRandomRetriever("dense_vector_field", 256);
        var rewritten = original.doRewrite(queryRewriteContext);
        assertSame(original, rewritten);
        assertCompoundRetriever(original, rewritten);

        // will assert that the rewrite happened without assertion errors
        List<ScoreDoc[]> docs = new ArrayList<>();
        docs.add(new ScoreDoc[] {});
        var result = original.combineInnerRetrieverResults(docs, false);
        assertEquals(0, result.length);
    }

    public void testMmrResultDiversification() {
        var queryRewriteContext = getQueryRewriteContext();
        var retriever = new DiversifyRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "dense_vector_field",
            10,
            3,
            new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f }),
            0.3f
        );

        // run the rewrite to set the internal diversification context
        retriever.doRewrite(queryRewriteContext);

        List<ScoreDoc[]> docs = new ArrayList<>();
        ScoreDoc[] hits = getTestSearchHits();
        docs.add(hits);

        var result = retriever.combineInnerRetrieverResults(docs, false);

        assertEquals(3, result.length);
        assertEquals(1, result[0].rank);
        assertEquals(3, result[1].rank);
        assertEquals(6, result[2].rank);

        var retrieverWithoutRewrite = new DiversifyRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "dense_vector_field",
            10,
            3,
            new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f }),
            0.3f
        );

        ElasticsearchStatusException missingRewriteEx = assertThrows(
            ElasticsearchStatusException.class,
            () -> retrieverWithoutRewrite.combineInnerRetrieverResults(List.of(), false)
        );
        assertEquals("diversificationContext is not set. \"doRewrite\" should have been called beforehand.", missingRewriteEx.getMessage());
        assertEquals(500, missingRewriteEx.status().getStatus());

        retrieverWithoutRewrite.doRewrite(queryRewriteContext);

        var emptyDocs = retrieverWithoutRewrite.combineInnerRetrieverResults(List.of(), false);
        assertEquals(0, emptyDocs.length);

        docs.add(hits);
        ElasticsearchStatusException exMultipleDocs = assertThrows(
            ElasticsearchStatusException.class,
            () -> retrieverWithoutRewrite.combineInnerRetrieverResults(docs, false)
        );
        assertEquals("rank results must have only one result set", exMultipleDocs.getMessage());
        assertEquals(400, exMultipleDocs.status().getStatus());

        cleanDocsAndHits(docs, hits);
    }

    public void testThrowsExceptionOnBadFieldTypes() {
        var queryRewriteContext = getQueryRewriteContext();
        var retriever = new DiversifyRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "dense_vector_field",
            10,
            3,
            new VectorData(new float[] { 0.5f, 0.2f, 0.4f, 0.4f }),
            0.3f
        );

        // run the rewrite to set the internal diversification context
        retriever.doRewrite(queryRewriteContext);

        List<ScoreDoc[]> docs = new ArrayList<>();
        ScoreDoc[] hits = getTestNonVectorSearchHits();
        docs.add(hits);

        ElasticsearchStatusException badDocFieldEx = assertThrows(
            ElasticsearchStatusException.class,
            () -> retriever.combineInnerRetrieverResults(docs, false)
        );
        assertEquals(
            "Failed to retrieve vectors for field [dense_vector_field]. Is it a [dense_vector] field?",
            badDocFieldEx.getMessage()
        );
        assertEquals(400, badDocFieldEx.status().getStatus());

        cleanDocsAndHits(docs, hits);

        ScoreDoc[] hitsWithNoValues = getTestSearchHitsWithNoValues();
        docs.add(hitsWithNoValues);

        ElasticsearchStatusException docsWithNoValuesEx = assertThrows(
            ElasticsearchStatusException.class,
            () -> retriever.combineInnerRetrieverResults(docs, false)
        );
        assertEquals(
            "Failed to retrieve vectors for field [dense_vector_field]. Is it a [dense_vector] field?",
            docsWithNoValuesEx.getMessage()
        );
        assertEquals(400, docsWithNoValuesEx.status().getStatus());

        cleanDocsAndHits(docs, hitsWithNoValues);
    }

    private void cleanDocsAndHits(List<ScoreDoc[]> docs, ScoreDoc[] hits) {
        docs.clear();
        for (ScoreDoc hit : hits) {
            ((DiversifyRetrieverBuilder.RankDocWithSearchHit) hit).hit().decRef();
        }
        Arrays.fill(hits, null);
    }

    private ScoreDoc[] getTestSearchHits() {
        return new DiversifyRetrieverBuilder.RankDocWithSearchHit[] {
            getTestSearchHit(1, 1, 2.0f, new float[] { 0.4f, 0.2f, 0.4f, 0.4f }),
            getTestSearchHit(2, 2, 1.8f, new float[] { 0.4f, 0.2f, 0.3f, 0.3f }),
            getTestSearchHit(3, 0, 1.8f, new float[] { 0.4f, 0.1f, 0.3f, 0.3f }),
            getTestSearchHit(4, 0, 1.0f, new float[] { 0.1f, 0.9f, 0.5f, 0.9f }),
            getTestSearchHit(5, 1, 0.8f, new float[] { 0.1f, 0.9f, 0.5f, 0.9f }),
            getTestSearchHit(6, 1, 0.8f, new float[] { 0.05f, 0.05f, 0.05f, 0.05f }) };
    }

    private ScoreDoc[] getTestNonVectorSearchHits() {
        return new DiversifyRetrieverBuilder.RankDocWithSearchHit[] {
            getTestNonVectorSearchHit(1, 1, 2.0f),
            getTestNonVectorSearchHit(2, 2, 1.8f),
            getTestNonVectorSearchHit(3, 1, 1.8f) };
    }

    private ScoreDoc[] getTestSearchHitsWithNoValues() {
        return new DiversifyRetrieverBuilder.RankDocWithSearchHit[] {
            getTestSearchHitWithNoValue(1, 1, 2.0f),
            getTestSearchHitWithNoValue(2, 1, 1.8f),
            getTestSearchHitWithNoValue(3, 1, 1.8f) };
    }

    private DiversifyRetrieverBuilder.RankDocWithSearchHit getTestSearchHit(int rank, int docId, float score, float[] value) {
        SearchHit hit = new SearchHit(docId);
        hit.setDocumentField(new DocumentField("dense_vector_field", List.of(value)));
        DiversifyRetrieverBuilder.RankDocWithSearchHit doc = new DiversifyRetrieverBuilder.RankDocWithSearchHit(docId, score, 1, hit);
        doc.rank = rank;
        return doc;
    }

    private DiversifyRetrieverBuilder.RankDocWithSearchHit getTestNonVectorSearchHit(int rank, int docId, float score) {
        SearchHit hit = new SearchHit(docId);
        Object value = randomBoolean() ? randomAlphanumericOfLength(16) : generateRandomStringArray(8, 16, false);
        hit.setDocumentField(new DocumentField("dense_vector_field", List.of(value)));
        DiversifyRetrieverBuilder.RankDocWithSearchHit doc = new DiversifyRetrieverBuilder.RankDocWithSearchHit(docId, score, 1, hit);
        doc.rank = rank;
        return doc;
    }

    private DiversifyRetrieverBuilder.RankDocWithSearchHit getTestSearchHitWithNoValue(int rank, int docId, float score) {
        SearchHit hit = new SearchHit(docId);
        DiversifyRetrieverBuilder.RankDocWithSearchHit doc = new DiversifyRetrieverBuilder.RankDocWithSearchHit(docId, score, 1, hit);
        doc.rank = rank;
        return doc;
    }

    protected void assertCompoundRetriever(DiversifyRetrieverBuilder originalRetriever, RetrieverBuilder rewrittenRetriever) {
        assertTrue(rewrittenRetriever instanceof DiversifyRetrieverBuilder);
        DiversifyRetrieverBuilder actualRetrieverBuilder = (DiversifyRetrieverBuilder) rewrittenRetriever;
        assertEquals(originalRetriever.rankWindowSize(), actualRetrieverBuilder.rankWindowSize());
    }

    private static DiversifyRetrieverBuilder createRandomRetriever() {
        return createRandomRetriever(null, null);
    }

    private static DiversifyRetrieverBuilder createRandomRetriever(@Nullable String fieldName, @Nullable Integer vectorDimensions) {
        String field = fieldName == null ? "test_field" : fieldName;
        int rankWindowSize = randomIntBetween(1, 20);
        Integer size = randomBoolean() ? null : randomIntBetween(1, 20);

        // TODO - decide using float for byte here!
        VectorData queryVector = vectorDimensions == null
            ? randomBoolean() ? getRandomQueryVector() : null
            : getRandomQueryVector(vectorDimensions);
        Float lambda = randomFloatBetween(0.0f, 1.0f, true);
        CompoundRetrieverBuilder.RetrieverSource innerRetriever = getInnerRetriever();
        return new DiversifyRetrieverBuilder(
            innerRetriever,
            ResultDiversificationType.MMR,
            field,
            rankWindowSize,
            size,
            queryVector,
            lambda
        );
    }

    private static CompoundRetrieverBuilder.RetrieverSource getInnerRetriever() {
        return new CompoundRetrieverBuilder.RetrieverSource(TestRetrieverBuilder.createRandomTestRetrieverBuilder(), null);
    }

    private static VectorData getRandomQueryVector() {
        return getRandomQueryVector(null);
    }

    private static VectorData getRandomQueryVector(@Nullable Integer vectorDimensions) {
        int vectorSize = vectorDimensions == null ? randomIntBetween(5, 256) : vectorDimensions;
        float[] queryVector = new float[vectorSize];
        for (int i = 0; i < queryVector.length; i++) {
            queryVector[i] = randomFloatBetween(0.0f, 1.0f, true);
        }
        return new VectorData(queryVector);
    }

    private static ResolvedIndices createMockResolvedIndices(Map<String, List<String>> localIndexDenseVectorFields) {
        Map<Index, IndexMetadata> indexMetadata = new HashMap<>();

        for (var indexEntry : localIndexDenseVectorFields.entrySet()) {
            String indexName = indexEntry.getKey();
            List<String> denseVectorFields = indexEntry.getValue();

            Index index = new Index(indexName, randomAlphaOfLength(10));

            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index.getName())
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                        .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
                )
                .numberOfShards(1)
                .numberOfReplicas(0);

            List<String> denseVectorFieldsList = new ArrayList<>();
            for (String denseVectorField : denseVectorFields) {
                denseVectorFieldsList.add(
                    String.format(Locale.ROOT, "\"%s\": { \"type\": \"dense_vector\", \"dims\": 256 }", denseVectorField)
                );
            }
            String mapping = String.format(Locale.ROOT, "{ \"properties\": {%s}}", String.join(",", denseVectorFieldsList));
            indexMetadataBuilder.putMapping(mapping);
            indexMetadata.put(index, indexMetadataBuilder.build());
        }

        Map<String, OriginalIndices> remoteIndices = new HashMap<>();
        return new MockResolvedIndices(
            remoteIndices,
            new OriginalIndices(localIndexDenseVectorFields.keySet().toArray(new String[0]), IndicesOptions.DEFAULT),
            indexMetadata
        );
    }

    private QueryRewriteContext getQueryRewriteContext() {
        final String indexName = "test-index";
        final List<String> testDenseVectorFields = List.of("dense_vector_field");
        final ResolvedIndices resolvedIndices = createMockResolvedIndices(Map.of(indexName, testDenseVectorFields));
        final Index localIndex = resolvedIndices.getConcreteLocalIndices()[0];
        final Predicate<String> nameMatcher = testDenseVectorFields::contains;
        final MappingLookup mappingLookup = MappingLookup.fromMapping(getTestMapping());

        var indexMetadata = IndexMetadata.builder("index")
            .settings(
                indexSettings(IndexVersion.current(), 1, 1).put(
                    Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build()
                )
            )
            .build();

        return new QueryRewriteContext(
            parserConfig(),
            null,
            null,
            null,
            mappingLookup,
            Collections.emptyMap(),
            new IndexSettings(indexMetadata, Settings.EMPTY),
            TransportVersion.current(),
            RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
            localIndex,
            nameMatcher,
            null,
            null,
            () -> false,
            null,
            resolvedIndices,
            new PointInTimeBuilder(new BytesArray("pitid")),
            null,
            null,
            false,
            false
        );
    }

    private Mapping getTestMapping() {
        SourceFieldMapper sourceMapper = new SourceFieldMapper.Builder(null, Settings.EMPTY, false, false, false).setSynthetic().build();
        RootObjectMapper root = new RootObjectMapper.Builder("_doc").add(
            new DenseVectorFieldMapper.Builder("dense_vector_field", IndexVersion.current(), false, List.of())
        ).build(MapperBuilderContext.root(true, false));

        return new Mapping(root, new MetadataFieldMapper[] { sourceMapper }, Map.of());
    }
}
