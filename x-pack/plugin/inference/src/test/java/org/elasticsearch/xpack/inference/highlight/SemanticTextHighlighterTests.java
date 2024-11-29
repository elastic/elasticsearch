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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DocumentMapper;
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;

public class SemanticTextHighlighterTests extends MapperServiceTestCase {
    private MapperService mapperService;
    private DocumentMapper documentMapper;

    private static final String MAPPINGS = """
        {
            "_doc": {
                "properties": {
                    "field": {
                        "type": "text",
                        "copy_to": ["sparse_field", "dense_field"]
                    },
                    "sparse_field": {
                        "type": "semantic_text",
                        "inference_id": ".elser-2-elasticsearch",
                        "model_settings": {
                            "task_type": "sparse_embedding"
                        }
                    },
                    "dense_field": {
                        "type": "semantic_text",
                        "inference_id": ".multilingual-e5-small-elasticsearch",
                        "model_settings": {
                            "task_type": "text_embedding",
                            "dimensions": 384,
                            "similarity": "cosine",
                            "element_type": "float"
                        }
                    }
                }
            }
        }
        """;

    @Override
    protected Collection<? extends Plugin> getPlugins() {
        return List.of(new InferencePlugin(Settings.EMPTY));
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        mapperService = createMapperService(MAPPINGS);
        documentMapper = mapperService.documentMapper();
    }

    private void assertHighlightOneDoc(ShardSearchRequest request, SourceToParse source, String fieldName, String[] expectedPassages)
        throws Exception {
        SemanticTextFieldMapper fieldMapper = (SemanticTextFieldMapper) mapperService.mappingLookup().getMapper(fieldName);
        var doc = documentMapper.parse(source);
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
                Mockito.when(fetchContext.request()).thenReturn(request);
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
                    var highlightContext = new HighlightBuilder().field(fieldName, 512, 1).highlighterType("semantic").build(execContext);

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
                        System.out.println(Strings.toString(result, true, true));
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

    public void testDenseVector() throws Exception {
        float[] vector = new float[] {
            0.09475211f,
            0.044564713f,
            -0.04378501f,
            -0.07908551f,
            0.04332011f,
            -0.03891992f,
            -0.0062305215f,
            0.024245035f,
            -0.008976331f,
            0.032832284f,
            0.052760173f,
            0.008123907f,
            0.09049037f,
            -0.01637332f,
            -0.054353267f,
            0.00771307f,
            0.08545496f,
            -0.079716265f,
            -0.045666866f,
            -0.04369993f,
            0.009189822f,
            -0.013782891f,
            -0.07701858f,
            0.037278354f,
            0.049807206f,
            0.078036495f,
            -0.059533164f,
            0.051413406f,
            0.040234447f,
            -0.038139492f,
            -0.085189626f,
            -0.045546446f,
            0.0544375f,
            -0.05604156f,
            0.057408098f,
            0.041913517f,
            -0.037348013f,
            -0.025998272f,
            0.08486864f,
            -0.046678443f,
            0.0041820924f,
            0.007514462f,
            0.06424746f,
            0.044233218f,
            0.103267275f,
            0.014130771f,
            -0.049954403f,
            0.04226959f,
            -0.08346965f,
            -0.01639249f,
            -0.060537644f,
            0.04546336f,
            0.012866155f,
            0.05375096f,
            0.036775924f,
            -0.0762226f,
            -0.037304543f,
            -0.05692274f,
            -0.055807598f,
            0.0040082196f,
            0.059259634f,
            0.012022011f,
            -8.0863154E-4f,
            0.0070405705f,
            0.050255686f,
            0.06810016f,
            0.017190414f,
            0.051975194f,
            -0.051436286f,
            0.023408439f,
            -0.029802637f,
            0.034137156f,
            -0.004660689f,
            -0.0442122f,
            0.019065322f,
            0.030806554f,
            0.0064652697f,
            -0.066789865f,
            0.057111286f,
            0.009412479f,
            -0.041444767f,
            -0.06807582f,
            -0.085881524f,
            0.04901128f,
            -0.047871742f,
            0.06328623f,
            0.040418074f,
            -0.081432894f,
            0.058384005f,
            0.006206527f,
            0.045801315f,
            0.037274595f,
            -0.054337103f,
            -0.06755516f,
            -0.07396888f,
            -0.043732334f,
            -0.052053086f,
            0.03210978f,
            0.048101492f,
            -0.083828256f,
            0.05205026f,
            -0.048474856f,
            0.029116616f,
            -0.10924888f,
            0.003796487f,
            0.030567763f,
            0.026949523f,
            -0.052353345f,
            0.043198872f,
            -0.09456988f,
            -0.05711594f,
            -2.2292069E-4f,
            0.032972734f,
            0.054394923f,
            -0.0767535f,
            -0.02710579f,
            -0.032135617f,
            -0.01732382f,
            0.059442326f,
            -0.07686165f,
            0.07104082f,
            -0.03090021f,
            -0.05450075f,
            -0.038997203f,
            -0.07045443f,
            0.00483161f,
            0.010933604f,
            0.020874644f,
            0.037941266f,
            0.019729063f,
            0.06178368f,
            0.013503478f,
            -0.008584046f,
            0.045592044f,
            0.05528768f,
            0.11568184f,
            0.0041300594f,
            0.015404516f,
            -3.8067883E-4f,
            -0.06365399f,
            -0.07826643f,
            0.061575573f,
            -0.060548335f,
            0.05706082f,
            0.042301804f,
            0.052173313f,
            0.07193179f,
            -0.03839231f,
            0.0734415f,
            -0.045380164f,
            0.02832276f,
            0.003745178f,
            0.058844633f,
            0.04307504f,
            0.037800383f,
            -0.031050054f,
            -0.06856359f,
            -0.059114788f,
            -0.02148857f,
            0.07854358f,
            -0.03253363f,
            -0.04566468f,
            -0.019933948f,
            -0.057993464f,
            -0.08677458f,
            -0.06626883f,
            0.031657256f,
            0.101128764f,
            -0.08050056f,
            -0.050226066f,
            -0.014335166f,
            0.050344367f,
            -0.06851419f,
            0.008698909f,
            -0.011893435f,
            0.07741272f,
            -0.059579294f,
            0.03250109f,
            0.058700256f,
            0.046834726f,
            -0.035081457f,
            -0.0043140925f,
            -0.09764087f,
            -0.0034994273f,
            -0.034056358f,
            -0.019066337f,
            -0.034376107f,
            0.012964423f,
            0.029291175f,
            -0.012090671f,
            0.021585712f,
            0.028859599f,
            -0.04391145f,
            -0.071166754f,
            -0.031040335f,
            0.02808108f,
            -0.05621317f,
            0.06543945f,
            0.10094665f,
            0.041057374f,
            -0.03222324f,
            -0.063366964f,
            0.064944476f,
            0.023641933f,
            0.06806713f,
            0.06806097f,
            -0.08220105f,
            0.04148528f,
            -0.09254079f,
            0.044620737f,
            0.05526614f,
            -0.03849534f,
            -0.04722273f,
            0.0670776f,
            -0.024274077f,
            -0.016903497f,
            0.07584147f,
            0.04760533f,
            -0.038843267f,
            -0.028365409f,
            0.08022705f,
            -0.039916333f,
            0.049067073f,
            -0.030701574f,
            -0.057169467f,
            0.043025102f,
            0.07109674f,
            -0.047296863f,
            -0.047463104f,
            0.040868305f,
            -0.04409507f,
            -0.034977127f,
            -0.057109762f,
            -0.08616165f,
            -0.03486079f,
            -0.046201482f,
            0.025963873f,
            0.023392359f,
            0.09594902f,
            -0.007847159f,
            -0.021231368f,
            0.009007263f,
            0.0032713825f,
            -0.06876065f,
            0.03169641f,
            -7.2582875E-4f,
            -0.07049708f,
            0.03900843f,
            -0.0075472407f,
            0.05184822f,
            0.06452079f,
            -0.09832754f,
            -0.012775799f,
            -0.03925948f,
            -0.029761659f,
            0.0065437574f,
            0.0815465f,
            0.0411695f,
            -0.0702844f,
            -0.009533786f,
            0.07024532f,
            0.0098710675f,
            0.09915362f,
            0.0415453f,
            0.050641853f,
            0.047463298f,
            -0.058609713f,
            -0.029499197f,
            -0.05100956f,
            -0.03441709f,
            -0.06348122f,
            0.014784361f,
            0.056317374f,
            -0.10280704f,
            -0.04008354f,
            -0.018926824f,
            0.08832836f,
            0.124804f,
            -0.047645308f,
            -0.07122146f,
            -9.886527E-4f,
            0.03850324f,
            0.048501793f,
            0.07072816f,
            0.06566776f,
            -0.013678872f,
            0.010010848f,
            0.06483413f,
            -0.030036367f,
            -0.029748922f,
            -0.007482364f,
            -0.05180385f,
            0.03698522f,
            -0.045453787f,
            0.056604166f,
            0.029394176f,
            0.028589265f,
            -0.012185886f,
            -0.06919616f,
            0.0711641f,
            -0.034055933f,
            -0.053101335f,
            0.062319f,
            0.021600349f,
            -0.038718067f,
            0.060814686f,
            0.05087301f,
            -0.020297311f,
            0.016493896f,
            0.032162152f,
            0.046740912f,
            0.05461355f,
            -0.07024665f,
            0.025609337f,
            -0.02504801f,
            0.06765588f,
            -0.032994855f,
            -0.037897404f,
            -0.045783922f,
            -0.05689299f,
            -0.040437017f,
            -0.07904339f,
            -0.031415287f,
            -0.029216278f,
            0.017395392f,
            0.03449264f,
            -0.025653394f,
            -0.06283088f,
            0.049027324f,
            0.016229525f,
            -0.00985347f,
            -0.053974394f,
            -0.030257035f,
            0.04325515f,
            -0.012293731f,
            -0.002446129f,
            -0.05567076f,
            0.06374684f,
            -0.03153897f,
            -0.04475149f,
            0.018582936f,
            0.025716115f,
            -0.061778374f,
            0.04196277f,
            -0.04134671f,
            -0.07396272f,
            0.05846184f,
            0.006558759f,
            -0.09745666f,
            0.07587805f,
            0.0137483915f,
            -0.100933895f,
            0.032008193f,
            0.04293283f,
            0.017870268f,
            0.032806385f,
            -0.0635923f,
            -0.019672254f,
            0.022225974f,
            0.04304554f,
            -0.06043949f,
            -0.0285274f,
            0.050868835f,
            0.057003833f,
            0.05740866f,
            0.020068677f,
            -0.034312245f,
            -0.021671802f,
            0.014769731f,
            -0.07328285f,
            -0.009586734f,
            0.036420938f,
            -0.022188472f,
            -0.008200541f,
            -0.010765854f,
            -0.06949713f,
            -0.07555878f,
            0.045306854f,
            -0.05424466f,
            -0.03647476f,
            0.06266633f,
            0.08346125f,
            0.060288202f,
            0.0548457f };
        KnnVectorQueryBuilder knnQuery = new KnnVectorQueryBuilder("dense_field.chunks.embeddings", vector, 10, 10, null);
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("dense_field.chunks", knnQuery, ScoreMode.Max);
        var shardRequest = createShardSearchRequest(nestedQueryBuilder);
        var sourceToParse = new SourceToParse(
            "0",
            Streams.readFully(SemanticTextHighlighterTests.class.getResourceAsStream("moby-dick.json")),
            XContentType.JSON
        );
        assertHighlightOneDoc(shardRequest, sourceToParse, "dense_field", new String[0]);
    }
}
