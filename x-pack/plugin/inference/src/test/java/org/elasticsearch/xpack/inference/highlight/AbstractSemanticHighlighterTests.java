/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.highlight;

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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.subphase.highlight.FieldHighlightContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.SearchHighlightContext;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.InferencePlugin;
import org.elasticsearch.xpack.inference.mapper.SemanticFieldMapper;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;

public abstract class AbstractSemanticHighlighterTests extends MapperServiceTestCase {
    private static final String SEMANTIC_FIELD = "field-semantic";
    private static final String SEMANTIC_FIELD_DISK_BBQ = "field-semantic-disk_bbq";

    final MapperService mapperService;
    final SourceToParse sourceToParse;
    final Map<String, Object> queryData;

    @SuppressWarnings("this-escape")
    public AbstractSemanticHighlighterTests(Settings settings, String mappings, SourceToParse sourceToParse, Map<String, Object> queryData)
        throws IOException {
        this.mapperService = createMapperService(IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings), settings, mappings);
        this.sourceToParse = sourceToParse;
        this.queryData = queryData;
    }

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY));
    }

    @SuppressWarnings("unchecked")
    public void testDenseVector() throws Exception {
        float[] vector = readDenseVector(queryData.get("embeddings"));
        var fieldType = (SemanticFieldMapper.SemanticFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD);
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

        String[] expectedScorePassages = ((List<String>) queryData.get("expected_by_score")).toArray(String[]::new);
        for (int i = 0; i < expectedScorePassages.length; i++) {
            assertHighlightOneDoc(
                mapperService,
                createSearchExecutionContext(mapperService),
                shardRequest,
                sourceToParse,
                SEMANTIC_FIELD,
                i + 1,
                HighlightBuilder.Order.SCORE,
                Arrays.copyOfRange(expectedScorePassages, 0, i + 1)
            );
        }

        String[] expectedOffsetPassages = ((List<String>) queryData.get("expected_by_offset")).toArray(String[]::new);
        assertHighlightOneDoc(
            mapperService,
            createSearchExecutionContext(mapperService),
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD,
            expectedOffsetPassages.length,
            HighlightBuilder.Order.NONE,
            expectedOffsetPassages
        );
    }

    @SuppressWarnings("unchecked")
    public void testDenseVectorWithSimilarityThreshold() throws Exception {
        float[] vector = readDenseVector(queryData.get("embeddings"));
        var fieldType = (SemanticFieldMapper.SemanticFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD);

        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            vector,
            10,
            10,
            10f,
            null,
            0.85f
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(fieldType.getChunksField().fullPath(), knnQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);

        String[] expectedPassages = ((List<String>) queryData.get("expected_with_similarity_threshold")).toArray(String[]::new);
        assertHighlightOneDoc(
            mapperService,
            createSearchExecutionContext(mapperService),
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD,
            expectedPassages.length,
            HighlightBuilder.Order.SCORE,
            expectedPassages
        );
    }

    @SuppressWarnings("unchecked")
    public void testDenseVectorWithDiskBBQandSimilarityThreshold() throws Exception {
        float[] vector = readDenseVector(queryData.get("embeddings"));
        var fieldType = (SemanticFieldMapper.SemanticFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_DISK_BBQ);

        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            vector,
            10,
            10,
            10f,
            null,
            0.85f
        );
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(fieldType.getChunksField().fullPath(), knnQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);

        String[] expectedPassages = ((List<String>) queryData.get("expected_with_similarity_threshold")).toArray(String[]::new);
        assertHighlightOneDoc(
            mapperService,
            createSearchExecutionContext(mapperService),
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD_DISK_BBQ,
            expectedPassages.length,
            HighlightBuilder.Order.SCORE,
            expectedPassages
        );
    }

    @SuppressWarnings("unchecked")
    public void testDenseVectorWithDiskBBQ() throws Exception {
        float[] vector = readDenseVector(queryData.get("embeddings"));
        var fieldType = (SemanticFieldMapper.SemanticFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD_DISK_BBQ);

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

        String[] expectedScorePassages = ((List<String>) queryData.get("expected_by_score")).toArray(String[]::new);
        for (int i = 0; i < expectedScorePassages.length; i++) {
            assertHighlightOneDoc(
                mapperService,
                createSearchExecutionContext(mapperService),
                shardRequest,
                sourceToParse,
                SEMANTIC_FIELD_DISK_BBQ,
                i + 1,
                HighlightBuilder.Order.SCORE,
                Arrays.copyOfRange(expectedScorePassages, 0, i + 1)
            );
        }

        String[] expectedOffsetPassages = ((List<String>) queryData.get("expected_by_offset")).toArray(String[]::new);
        assertHighlightOneDoc(
            mapperService,
            createSearchExecutionContext(mapperService),
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD_DISK_BBQ,
            expectedOffsetPassages.length,
            HighlightBuilder.Order.NONE,
            expectedOffsetPassages
        );
    }

    @SuppressWarnings("unchecked")
    public void testNoSemanticField() throws Exception {
        float[] vector = readDenseVector(queryData.get("embeddings"));
        var fieldType = (SemanticFieldMapper.SemanticFieldType) mapperService.mappingLookup().getFieldType(SEMANTIC_FIELD);

        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(
            fieldType.getEmbeddingsField().fullPath(),
            vector,
            10,
            10,
            10f,
            null,
            null
        );
        var query = new BoolQueryBuilder().should(knnQuery).should(new MatchAllQueryBuilder());
        var shardRequest = createShardSearchRequest(query);
        var sourceToParse = new SourceToParse("0", new BytesArray("{}"), XContentType.JSON);
        assertHighlightOneDoc(
            mapperService,
            createSearchExecutionContext(mapperService),
            shardRequest,
            sourceToParse,
            SEMANTIC_FIELD,
            10,
            HighlightBuilder.Order.SCORE,
            new String[0]
        );
    }

    private MapperService createMapperService(IndexVersion indexVersion, Settings settings, String mappings) throws IOException {
        var mapperService = createMapperService(indexVersion, settings, mapping(b -> {}));
        merge(mapperService, mappings);
        return mapperService;
    }

    private static float[] readDenseVector(Object value) {
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

    static void assertHighlightOneDoc(
        MapperService mapperService,
        SearchExecutionContext execContext,
        ShardSearchRequest request,
        SourceToParse source,
        String fieldName,
        int numFragments,
        HighlightBuilder.Order order,
        String[] expectedPassages
    ) throws Exception {
        SemanticFieldMapper fieldMapper = (SemanticFieldMapper) mapperService.mappingLookup().getMapper(fieldName);
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
                SemanticTextHighlighter highlighter = new SemanticTextHighlighter();
                var luceneQuery = execContext.toQuery(request.source().query()).query();
                FetchContext fetchContext = mock(FetchContext.class);
                Mockito.when(fetchContext.highlight()).thenReturn(new SearchHighlightContext(Collections.emptyList()));
                Mockito.when(fetchContext.query()).thenReturn(luceneQuery);
                Mockito.when(fetchContext.getSearchExecutionContext()).thenReturn(execContext);

                FetchSubPhase.HitContext hitContext = new FetchSubPhase.HitContext(
                    new SearchHit(docID),
                    getOnlyLeafReader(reader).getContext(),
                    docID,
                    Map.of(),
                    Source.fromBytes(source.source().originalBytes()),
                    new RankDoc(docID, Float.NaN, 0)
                );
                try {
                    var highlightContext = new HighlightBuilder().field(fieldName, 0, numFragments)
                        .order(order)
                        .highlighterType(SemanticTextHighlighter.NAME)
                        .build(execContext);

                    for (var fieldContext : highlightContext.fields()) {
                        FieldHighlightContext context = new FieldHighlightContext(
                            fieldName,
                            fieldContext,
                            fieldMapper.fieldType(),
                            fetchContext,
                            hitContext,
                            luceneQuery,
                            new HashMap<>()
                        );
                        var result = highlighter.highlight(context);
                        if (result == null) {
                            assertThat(expectedPassages.length, equalTo(0));
                        } else {
                            assertThat(result.fragments().length, equalTo(expectedPassages.length));
                            for (int i = 0; i < result.fragments().length; i++) {
                                assertThat(result.fragments()[i].string(), equalTo(expectedPassages[i]));
                            }
                        }
                    }
                } finally {
                    hitContext.hit().decRef();
                }
            }
        }
    }

    private static SearchRequest createSearchRequest(QueryBuilder queryBuilder) {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder());
        request.allowPartialSearchResults(false);
        request.source().query(queryBuilder);
        return request;
    }

    static ShardSearchRequest createShardSearchRequest(QueryBuilder queryBuilder) {
        SearchRequest request = createSearchRequest(queryBuilder);
        return new ShardSearchRequest(OriginalIndices.NONE, request, new ShardId("index", "index", 0), 0, 1, AliasFilter.EMPTY, 1, 0, null);
    }

    static SourceToParse sourceFromFile(InputStream inputStream) throws IOException {
        try (var in = new GZIPInputStream(inputStream)) {
            return new SourceToParse("0", new BytesArray(new BytesRef(in.readAllBytes())), XContentType.JSON);
        }
    }
}
