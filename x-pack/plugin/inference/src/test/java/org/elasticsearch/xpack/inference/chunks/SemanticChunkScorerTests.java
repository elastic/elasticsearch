/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.chunks;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.InferenceMetadataFieldsMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.inference.WeightedToken;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.common.chunks.ScoredChunk;
import org.elasticsearch.xpack.core.ml.search.SparseVectorQueryBuilder;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.common.chunks.SemanticChunkScorer;
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;

public class SemanticChunkScorerTests extends MapperServiceTestCase {
    private static final String SEMANTIC_FIELD_E5 = "body-e5";
    private static final String SEMANTIC_FIELD_ELSER = "body-elser";

    private final boolean useLegacyFormat;
    private final Map<String, Object> queries;

    public SemanticChunkScorerTests(boolean useLegacyFormat) throws IOException {
        this.useLegacyFormat = useLegacyFormat;
        var input = Streams.readFully(SemanticChunkScorerTests.class.getResourceAsStream("queries.json"));
        this.queries = XContentHelper.convertToMap(input, false, XContentType.JSON).v2();
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return List.of(new Object[] { true }, new Object[] { false });
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY));
    }

    @SuppressWarnings("unchecked")
    public void testDenseVector() throws Exception {
        var mapperService = createDefaultMapperService(useLegacyFormat);
        Map<String, Object> queryMap = (Map<String, Object>) queries.get("dense_vector_1");
        float[] vector = readDenseVector(queryMap.get("embeddings"));
        var fieldType = (SemanticTextFieldMapper.SemanticTextFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_E5);
        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            vector,
            10,
            10,
            10f,
            null,
            null
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(fieldType.getChunksField().fullPath(), knnQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);
        var sourceToParse = new SourceToParse("0", readSampleDoc(useLegacyFormat), XContentType.JSON);

        String[] expectedScoreChunks = ((List<String>) queryMap.get("expected_by_score")).toArray(String[]::new);
        for (int i = 0; i < expectedScoreChunks.length; i++) {
            assertScoreChunksOneDoc(
                mapperService,
                shardRequest,
                sourceToParse,
                SEMANTIC_FIELD_E5,
                i + 1,
                Arrays.copyOfRange(expectedScoreChunks, 0, i + 1)
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void testSparseVector() throws Exception {
        var mapperService = createDefaultMapperService(useLegacyFormat);
        Map<String, Object> queryMap = (Map<String, Object>) queries.get("sparse_vector_1");
        List<WeightedToken> tokens = readSparseVector(queryMap.get("embeddings"));
        var fieldType = (SemanticTextFieldMapper.SemanticTextFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_ELSER);
        SparseVectorQueryBuilder sparseQuery = new SparseVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            tokens,
            null,
            null,
            false,
            null
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(fieldType.getChunksField().fullPath(), sparseQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);
        var sourceToParse = new SourceToParse("0", readSampleDoc(useLegacyFormat), XContentType.JSON);

        String[] expectedScoreChunks = ((List<String>) queryMap.get("expected_by_score")).toArray(String[]::new);
        for (int i = 0; i < expectedScoreChunks.length; i++) {
            assertScoreChunksOneDoc(
                mapperService,
                shardRequest,
                sourceToParse,
                SEMANTIC_FIELD_ELSER,
                i + 1,
                Arrays.copyOfRange(expectedScoreChunks, 0, i + 1)
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void testNoSemanticField() throws Exception {
        var mapperService = createDefaultMapperService(useLegacyFormat);
        Map<String, Object> queryMap = (Map<String, Object>) queries.get("sparse_vector_1");
        List<WeightedToken> tokens = readSparseVector(queryMap.get("embeddings"));
        var fieldType = (SemanticTextFieldMapper.SemanticTextFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_ELSER);
        SparseVectorQueryBuilder sparseQuery = new SparseVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            tokens,
            null,
            null,
            false,
            null
        );
        var query = new BoolQueryBuilder().should(sparseQuery).should(new MatchAllQueryBuilder());
        var shardRequest = createShardSearchRequest(query);
        var sourceToParse = new SourceToParse("0", new BytesArray("{}"), XContentType.JSON);
        assertScoreChunksOneDoc(mapperService, shardRequest, sourceToParse, SEMANTIC_FIELD_ELSER, 10, new String[0]);
    }

    public void testEmptyInferenceText() throws Exception {
        var mapperService = createDefaultMapperService(useLegacyFormat);
        var fieldType = (SemanticTextFieldMapper.SemanticTextFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_E5);
        var sourceToParse = new SourceToParse("0", readSampleDoc(useLegacyFormat), XContentType.JSON);

        var doc = mapperService.documentMapper().parse(sourceToParse);
        assertNull(doc.dynamicMappingsUpdate());
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig(new StandardAnalyzer());
            RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
            iw.addDocuments(doc.docs());
            try (DirectoryReader reader = wrapInMockESDirectoryReader(iw.getReader())) {
                IndexSearcher searcher = newSearcher(reader);
                iw.close();
                TopDocs topDocs = searcher.search(Queries.newNonNestedFilter(IndexVersion.current()), 1, Sort.INDEXORDER);
                assertThat(topDocs.totalHits.value(), equalTo(1L));
                int docID = topDocs.scoreDocs[0].doc;

                var execContext = createSearchExecutionContext(mapperService);
                SearchContext searchContext = mock(SearchContext.class);
                Mockito.when(searchContext.getSearchExecutionContext()).thenReturn(execContext);
                Mockito.when(searchContext.searcher()).thenReturn((org.elasticsearch.search.internal.ContextIndexSearcher) searcher);
                Mockito.when(searchContext.query()).thenReturn(new MatchAllQueryBuilder().toQuery(execContext));

                SemanticChunkScorer scorer = new SemanticChunkScorer(searchContext);
                SearchHit hit = new SearchHit(docID);
                hit.sourceRef(sourceToParse.source());

                // Test with null inferenceText
                List<ScoredChunk> chunks = scorer.scoreChunks(fieldType, hit, null, 5);
                assertThat(chunks.size(), equalTo(0));

                // Test with empty inferenceText
                chunks = scorer.scoreChunks(fieldType, hit, "", 5);
                assertThat(chunks.size(), equalTo(0));

                // Test with whitespace-only inferenceText
                chunks = scorer.scoreChunks(fieldType, hit, "   ", 5);
                assertThat(chunks.size(), equalTo(0));

                hit.decRef();
            }
        }
    }

    public void testMaxResultsLimit() throws Exception {
        var mapperService = createDefaultMapperService(useLegacyFormat);
        Map<String, Object> queryMap = (Map<String, Object>) queries.get("dense_vector_1");
        float[] vector = readDenseVector(queryMap.get("embeddings"));
        var fieldType = (SemanticTextFieldMapper.SemanticTextFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_E5);
        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            vector,
            10,
            10,
            10f,
            null,
            null
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(fieldType.getChunksField().fullPath(), knnQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);
        var sourceToParse = new SourceToParse("0", readSampleDoc(useLegacyFormat), XContentType.JSON);

        String[] expectedScoreChunks = ((List<String>) queryMap.get("expected_by_score")).toArray(String[]::new);

        // Test with maxResults = 1
        assertScoreChunksOneDoc(
            mapperService,
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD_E5,
            1,
            Arrays.copyOfRange(expectedScoreChunks, 0, 1)
        );

        // Test with maxResults = 3
        assertScoreChunksOneDoc(
            mapperService,
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD_E5,
            3,
            Arrays.copyOfRange(expectedScoreChunks, 0, 3)
        );

        // Test with maxResults greater than available chunks
        assertScoreChunksOneDoc(mapperService, shardRequest, sourceToParse, SEMANTIC_FIELD_E5, 100, expectedScoreChunks);
    }

    private MapperService createDefaultMapperService(boolean useLegacyFormat) throws IOException {
        var mappings = Streams.readFully(SemanticChunkScorerTests.class.getResourceAsStream("mappings.json"));
        var settings = Settings.builder()
            .put(InferenceMetadataFieldsMapper.USE_LEGACY_SEMANTIC_TEXT_FORMAT.getKey(), useLegacyFormat)
            .build();
        return createMapperService(settings, mappings.utf8ToString());
    }

    private float[] readDenseVector(Object value) {
        if (value instanceof List<?> lst) {
            float[] res = new float[lst.size()];
            int pos = 0;
            for (var obj : lst) {
                if (obj instanceof Number number) {
                    res[pos++] = number.floatValue();
                } else {
                    throw new IllegalArgumentException("Expected number, got " + obj.getClass().getSimpleName());
                }
            }
            return res;
        }
        throw new IllegalArgumentException("Expected list, got " + value.getClass().getSimpleName());
    }

    private List<WeightedToken> readSparseVector(Object value) {
        if (value instanceof Map<?, ?> map) {
            List<WeightedToken> res = new ArrayList<>();
            for (var entry : map.entrySet()) {
                if (entry.getValue() instanceof Number number) {
                    res.add(new WeightedToken((String) entry.getKey(), number.floatValue()));
                } else {
                    throw new IllegalArgumentException("Expected number, got " + entry.getValue().getClass().getSimpleName());
                }
            }
            return res;
        }
        throw new IllegalArgumentException("Expected map, got " + value.getClass().getSimpleName());
    }

    private void assertScoreChunksOneDoc(
        MapperService mapperService,
        ShardSearchRequest request,
        SourceToParse source,
        String fieldName,
        int maxResults,
        String[] expectedChunks
    ) throws Exception {
        SemanticTextFieldMapper fieldMapper = (SemanticTextFieldMapper) mapperService.mappingLookup().getMapper(fieldName);
        var doc = mapperService.documentMapper().parse(source);
        assertNull(doc.dynamicMappingsUpdate());
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig(new StandardAnalyzer());
            RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
            iw.addDocuments(doc.docs());
            try (DirectoryReader reader = wrapInMockESDirectoryReader(iw.getReader())) {
                IndexSearcher searcher = newSearcher(reader);
                iw.close();
                TopDocs topDocs = searcher.search(Queries.newNonNestedFilter(IndexVersion.current()), 1, Sort.INDEXORDER);
                assertThat(topDocs.totalHits.value(), equalTo(1L));
                int docID = topDocs.scoreDocs[0].doc;

                var execContext = createSearchExecutionContext(mapperService);
                var luceneQuery = execContext.toQuery(request.source().query()).query();
                SearchContext searchContext = mock(SearchContext.class);
                Mockito.when(searchContext.getSearchExecutionContext()).thenReturn(execContext);
                Mockito.when(searchContext.searcher()).thenReturn((org.elasticsearch.search.internal.ContextIndexSearcher) searcher);
                Mockito.when(searchContext.query()).thenReturn(luceneQuery);

                SemanticChunkScorer scorer = new SemanticChunkScorer(searchContext);
                SearchHit hit = new SearchHit(docID);
                hit.sourceRef(source.source());

                try {
                    // Use "Paris" as the inference text for scoring
                    List<ScoredChunk> scoredChunks = scorer.scoreChunks(fieldMapper.fieldType(), hit, "Paris", maxResults);

                    if (expectedChunks.length == 0) {
                        assertThat(scoredChunks.size(), equalTo(0));
                    } else {
                        assertThat(scoredChunks.size(), equalTo(expectedChunks.length));
                        for (int i = 0; i < scoredChunks.size(); i++) {
                            assertThat(scoredChunks.get(i).content(), equalTo(expectedChunks[i]));
                            // Verify that scores are in descending order
                            if (i > 0) {
                                assertTrue(
                                    "Chunks should be ordered by score descending",
                                    scoredChunks.get(i - 1).score() >= scoredChunks.get(i).score()
                                );
                            }
                        }
                    }
                } finally {
                    hit.decRef();
                }
            }
        }
    }

    private SearchRequest createSearchRequest(QueryBuilder queryBuilder) {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder());
        request.allowPartialSearchResults(false);
        request.source().query(queryBuilder);
        return request;
    }

    private ShardSearchRequest createShardSearchRequest(QueryBuilder queryBuilder) {
        SearchRequest request = createSearchRequest(queryBuilder);
        return new ShardSearchRequest(OriginalIndices.NONE, request, new ShardId("index", "index", 0), 0, 1, AliasFilter.EMPTY, 1, 0, null);
    }

    private BytesReference readSampleDoc(boolean useLegacyFormat) throws IOException {
        String fileName = useLegacyFormat ? "sample-doc-legacy.json.gz" : "sample-doc.json.gz";
        try (var in = new GZIPInputStream(SemanticChunkScorerTests.class.getResourceAsStream(fileName))) {
            return new BytesArray(new BytesRef(in.readAllBytes()));
        }
    }
}
