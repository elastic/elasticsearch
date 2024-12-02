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
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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
import org.elasticsearch.xpack.inference.mapper.SemanticTextFieldMapper;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;

public class SemanticTextHighlighterTests extends MapperServiceTestCase {
    private Map<String, Object> queries;

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY));
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        queries = XContentHelper.convertToMap(
            Streams.readFully(SemanticTextHighlighterTests.class.getResourceAsStream("queries.json")),
            false,
            XContentType.JSON
        ).v2();
    }

    public void testDenseVector() throws Exception {
        var mapperService = createDefaultMapperService();
        float[] vector = readDenseVector(queries.get("dense_vector_1"));
        var fieldType = (SemanticTextFieldMapper.SemanticTextFieldType) mapperService.mappingLookup().getFieldType("dense_field");
        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder(fieldType.getEmbeddingsField().fullPath(), vector, 10, 10, null);
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(fieldType.getChunksField().fullPath(), knnQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);
        var sourceToParse = new SourceToParse(
            "0",
            Streams.readFully(SemanticTextHighlighterTests.class.getResourceAsStream("sample-doc.json")),
            XContentType.JSON
        );

        assertHighlightOneDoc(mapperService, shardRequest, sourceToParse, "dense_field", 1, new String[] { "" });
    }

    private MapperService createDefaultMapperService() throws IOException {
        return createMapperService(
            Streams.readFully(SemanticTextHighlighterTests.class.getResourceAsStream("mappings.json")).utf8ToString()
        );
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

    private void assertHighlightOneDoc(
        MapperService mapperService,
        ShardSearchRequest request,
        SourceToParse source,
        String fieldName,
        int numFragments,
        String[] expectedPassages
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
                SemanticTextHighlighter highlighter = new SemanticTextHighlighter();
                var execContext = createSearchExecutionContext(mapperService);
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
                    Source.fromBytes(source.source()),
                    new RankDoc(docID, Float.NaN, 0)
                );
                try {
                    var highlightContext = new HighlightBuilder().field(fieldName, 0, numFragments)
                        .highlighterType("semantic")
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
                        assertThat(result.fragments().length, equalTo(expectedPassages.length));
                        for (int i = 0; i < result.fragments().length; i++) {
                            assertThat(result.fragments()[i].string(), equalTo(expectedPassages[i]));
                        }
                    }
                } finally {
                    hitContext.hit().decRef();
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
}
