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

public class ResultDiversificationRetrieverBuilderTests extends ESTestCase {

    public void testValidate() {
        SearchSourceBuilder source = new SearchSourceBuilder();

        // ensure type is MMR
        IllegalArgumentException ex = assertThrows(
            IllegalArgumentException.class,
            () -> new ResultDiversificationRetrieverBuilder(getInnerRetriever(), null, "test_field", 10, getRandomQueryVector(), 0.5f)
        );
        assertEquals("[diversify] diversification type must be set to [mmr]", ex.getMessage());

        // ensure lambda is within range and set
        var retrieverHighLambda = new ResultDiversificationRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "test_field",
            10,
            getRandomQueryVector(),
            2.0f
        );
        var validationLambda = retrieverHighLambda.validate(source, null, false, false);
        assertEquals(1, validationLambda.validationErrors().size());
        assertEquals(
            "[diversify] MMR result diversification must have a [lambda] between 0.0 and 1.0",
            validationLambda.validationErrors().getFirst()
        );

        var retrieverLowLambda = new ResultDiversificationRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "test_field",
            10,
            getRandomQueryVector(),
            -0.1f
        );
        validationLambda = retrieverLowLambda.validate(source, null, false, false);
        assertEquals(1, validationLambda.validationErrors().size());
        assertEquals(
            "[diversify] MMR result diversification must have a [lambda] between 0.0 and 1.0",
            validationLambda.validationErrors().getFirst()
        );

        var retrieverNullLambda = new ResultDiversificationRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "test_field",
            10,
            getRandomQueryVector(),
            null
        );
        validationLambda = retrieverNullLambda.validate(source, null, false, false);
        assertEquals(1, validationLambda.validationErrors().size());
        assertEquals(
            "[diversify] MMR result diversification must have a [lambda] between 0.0 and 1.0",
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
        var retriever = new ResultDiversificationRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "dense_vector_field",
            3,
            new float[] { 0.5f, 0.2f, 0.4f, 0.4f },
            0.3f
        );

        // run the rewrite to set the internal diversification context
        retriever.doRewrite(queryRewriteContext);

        List<ScoreDoc[]> docs = new ArrayList<>();
        ScoreDoc[] hits = getTestSearchHits();
        docs.add(hits);

        var result = retriever.combineInnerRetrieverResults(docs, false);

        assertEquals(3, result.length);
        assertEquals(1, result[0].doc);
        assertEquals(3, result[1].doc);
        assertEquals(6, result[2].doc);

        var retrieverWithoutRewrite = new ResultDiversificationRetrieverBuilder(
            getInnerRetriever(),
            ResultDiversificationType.MMR,
            "dense_vector_field",
            3,
            new float[] { 0.5f, 0.2f, 0.4f, 0.4f },
            0.3f
        );

        retrieverWithoutRewrite.doRewrite(queryRewriteContext);

        var emptyDocs = retrieverWithoutRewrite.combineInnerRetrieverResults(List.of(), false);
        assertEquals(0, emptyDocs.length);

        docs.add(hits);
        IllegalArgumentException exMultipleDocs = assertThrows(
            IllegalArgumentException.class,
            () -> retrieverWithoutRewrite.combineInnerRetrieverResults(docs, false)
        );
        assertEquals("rank results must have only one result set", exMultipleDocs.getMessage());

        // clean up
        docs.clear();
        for (ScoreDoc hit : hits) {
            ((ResultDiversificationRetrieverBuilder.RankDocWithSearchHit) hit).hit().decRef();
        }
        Arrays.fill(hits, null);
    }

    private ScoreDoc[] getTestSearchHits() {
        return new ResultDiversificationRetrieverBuilder.RankDocWithSearchHit[] {
            getTestSearchHit(1, 2.0f, new float[] { 0.4f, 0.2f, 0.4f, 0.4f }),
            getTestSearchHit(2, 1.8f, new float[] { 0.4f, 0.2f, 0.3f, 0.3f }),
            getTestSearchHit(3, 1.8f, new float[] { 0.4f, 0.1f, 0.3f, 0.3f }),
            getTestSearchHit(4, 1.0f, new float[] { 0.1f, 0.9f, 0.5f, 0.9f }),
            getTestSearchHit(5, 0.8f, new float[] { 0.1f, 0.9f, 0.5f, 0.9f }),
            getTestSearchHit(6, 0.8f, new float[] { 0.05f, 0.05f, 0.05f, 0.05f }) };
    }

    private ResultDiversificationRetrieverBuilder.RankDocWithSearchHit getTestSearchHit(int docId, float score, float[] value) {
        SearchHit hit = new SearchHit(docId);
        hit.setDocumentField(new DocumentField("dense_vector_field", List.of(value)));
        return new ResultDiversificationRetrieverBuilder.RankDocWithSearchHit(docId, score, 1, hit);
    }

    protected void assertCompoundRetriever(ResultDiversificationRetrieverBuilder originalRetriever, RetrieverBuilder rewrittenRetriever) {
        assertTrue(rewrittenRetriever instanceof ResultDiversificationRetrieverBuilder);
        ResultDiversificationRetrieverBuilder actualRetrieverBuilder = (ResultDiversificationRetrieverBuilder) rewrittenRetriever;
        assertEquals(originalRetriever.rankWindowSize(), actualRetrieverBuilder.rankWindowSize());
    }

    private static ResultDiversificationRetrieverBuilder createRandomRetriever() {
        return createRandomRetriever(null, null);
    }

    private static ResultDiversificationRetrieverBuilder createRandomRetriever(
        @Nullable String fieldName,
        @Nullable Integer vectorDimensions
    ) {
        String field = fieldName == null ? "test_field" : fieldName;
        int rankWindowSize = randomIntBetween(1, 20);
        float[] queryVector = vectorDimensions == null
            ? randomBoolean() ? getRandomQueryVector() : null
            : getRandomQueryVector(vectorDimensions);
        Float lambda = randomFloatBetween(0.0f, 1.0f, true);
        CompoundRetrieverBuilder.RetrieverSource innerRetriever = getInnerRetriever();
        return new ResultDiversificationRetrieverBuilder(
            innerRetriever,
            ResultDiversificationType.MMR,
            field,
            rankWindowSize,
            queryVector,
            lambda
        );
    }

    private static CompoundRetrieverBuilder.RetrieverSource getInnerRetriever() {
        return new CompoundRetrieverBuilder.RetrieverSource(TestRetrieverBuilder.createRandomTestRetrieverBuilder(), null);
    }

    private static float[] getRandomQueryVector() {
        return getRandomQueryVector(null);
    }

    private static float[] getRandomQueryVector(@Nullable Integer vectorDimensions) {
        int vectorSize = vectorDimensions == null ? randomIntBetween(5, 256) : vectorDimensions;
        float[] queryVector = new float[vectorSize];
        for (int i = 0; i < queryVector.length; i++) {
            queryVector[i] = randomFloatBetween(0.0f, 1.0f, true);
        }
        return queryVector;
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
