/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.percolator;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.action.percolate.PercolateShardRequest;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.analysis.AnalyzerProvider;
import org.elasticsearch.index.analysis.CharFilterFactory;
import org.elasticsearch.index.analysis.TokenFilterFactory;
import org.elasticsearch.index.analysis.TokenizerFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryParser;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBinaryParseElement;
import org.elasticsearch.search.aggregations.AggregationParseElement;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.highlight.HighlightPhase;
import org.elasticsearch.search.highlight.Highlighters;
import org.elasticsearch.search.sort.SortParseElement;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class PercolateDocumentParserTests extends ESTestCase {

    private MapperService mapperService;
    private PercolateDocumentParser parser;
    private QueryShardContext queryShardContext;
    private PercolateShardRequest request;

    @Before
    public void init() {
        IndexSettings indexSettings = new IndexSettings(new IndexMetaData.Builder("_index").settings(
                Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                        .build(),
                Settings.EMPTY, Collections.emptyList()
        );
        AnalysisService analysisService = new AnalysisService(indexSettings, Collections.<String, AnalyzerProvider>emptyMap(), Collections.<String, TokenizerFactory>emptyMap(), Collections.<String, CharFilterFactory>emptyMap(), Collections.<String, TokenFilterFactory>emptyMap());
        IndicesModule indicesModule = new IndicesModule();
        mapperService = new MapperService(indexSettings, analysisService, new SimilarityService(indexSettings, Collections.emptyMap()), indicesModule.getMapperRegistry(), () -> null);

        Map<String, QueryParser<?>> parsers = singletonMap("term", new TermQueryParser());
        IndicesQueriesRegistry indicesQueriesRegistry = new IndicesQueriesRegistry(indexSettings.getSettings(), parsers);

        queryShardContext = new QueryShardContext(indexSettings, null, null, null, mapperService, null, null, indicesQueriesRegistry);

        HighlightPhase highlightPhase = new HighlightPhase(Settings.EMPTY, new Highlighters());
        AggregatorParsers aggregatorParsers = new AggregatorParsers(Collections.emptySet(), Collections.emptySet());
        AggregationPhase aggregationPhase = new AggregationPhase(new AggregationParseElement(aggregatorParsers), new AggregationBinaryParseElement(aggregatorParsers));
        MappingUpdatedAction mappingUpdatedAction = Mockito.mock(MappingUpdatedAction.class);
        parser = new PercolateDocumentParser(
                highlightPhase, new SortParseElement(), aggregationPhase, mappingUpdatedAction
        );

        request = Mockito.mock(PercolateShardRequest.class);
        Mockito.when(request.shardId()).thenReturn(new ShardId(new Index("_index"), 0));
        Mockito.when(request.documentType()).thenReturn("type");
    }

    public void testParseDoc() throws Exception {
        XContentBuilder source = jsonBuilder().startObject()
                .startObject("doc")
                    .field("field1", "value1")
                .endObject()
                .endObject();
        Mockito.when(request.source()).thenReturn(source.bytes());

        PercolateContext context = new PercolateContext(request, new SearchShardTarget("_node", "_index", 0), mapperService);
        ParsedDocument parsedDocument = parser.parse(request, context, mapperService, queryShardContext);
        assertThat(parsedDocument.rootDoc().get("field1"), equalTo("value1"));
    }

    public void testParseDocAndOtherOptions() throws Exception {
        XContentBuilder source = jsonBuilder().startObject()
                .startObject("doc")
                    .field("field1", "value1")
                .endObject()
                .startObject("query")
                    .startObject("term").field("field1", "value1").endObject()
                .endObject()
                .field("track_scores", true)
                .field("size", 123)
                .startObject("sort").startObject("_score").endObject().endObject()
                .endObject();
        Mockito.when(request.source()).thenReturn(source.bytes());

        PercolateContext context = new PercolateContext(request, new SearchShardTarget("_node", "_index", 0), mapperService);
        ParsedDocument parsedDocument = parser.parse(request, context, mapperService, queryShardContext);
        assertThat(parsedDocument.rootDoc().get("field1"), equalTo("value1"));
        assertThat(context.percolateQuery(), equalTo(new TermQuery(new Term("field1", "value1"))));
        assertThat(context.trackScores(), is(true));
        assertThat(context.size(), is(123));
        assertThat(context.sort(), nullValue());
    }

    public void testParseDocSource() throws Exception {
        XContentBuilder source = jsonBuilder().startObject()
                .startObject("query")
                .startObject("term").field("field1", "value1").endObject()
                .endObject()
                .field("track_scores", true)
                .field("size", 123)
                .startObject("sort").startObject("_score").endObject().endObject()
                .endObject();
        XContentBuilder docSource = jsonBuilder().startObject()
                .field("field1", "value1")
                .endObject();
        Mockito.when(request.source()).thenReturn(source.bytes());
        Mockito.when(request.docSource()).thenReturn(docSource.bytes());

        PercolateContext context = new PercolateContext(request, new SearchShardTarget("_node", "_index", 0), mapperService);
        ParsedDocument parsedDocument = parser.parse(request, context, mapperService, queryShardContext);
        assertThat(parsedDocument.rootDoc().get("field1"), equalTo("value1"));
        assertThat(context.percolateQuery(), equalTo(new TermQuery(new Term("field1", "value1"))));
        assertThat(context.trackScores(), is(true));
        assertThat(context.size(), is(123));
        assertThat(context.sort(), nullValue());
    }

    public void testParseDocSourceAndSource() throws Exception {
        XContentBuilder source = jsonBuilder().startObject()
                .startObject("doc")
                .field("field1", "value1")
                .endObject()
                .startObject("query")
                .startObject("term").field("field1", "value1").endObject()
                .endObject()
                .field("track_scores", true)
                .field("size", 123)
                .startObject("sort").startObject("_score").endObject().endObject()
                .endObject();
        XContentBuilder docSource = jsonBuilder().startObject()
                .field("field1", "value1")
                .endObject();
        Mockito.when(request.source()).thenReturn(source.bytes());
        Mockito.when(request.docSource()).thenReturn(docSource.bytes());

        PercolateContext context = new PercolateContext(request, new SearchShardTarget("_node", "_index", 0), mapperService);
        try {
            parser.parse(request, context, mapperService, queryShardContext);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Can't specify the document to percolate in the source of the request and as document id"));
        }
    }

}
