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

package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.QueryFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryShardContextTests.getMockQueryShardContext;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FunctionScoreQueryFunctionTests extends ESTestCase {

    public static final String FIELD = "test";
    private Directory dir;
    private IndexWriter w;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    public static final String TEXT_1 = "There once was a runner named Dwight";
    public static final String TEXT_2 = "who could speed even faster than light.";
    public static final String TEXT_3 = "He set out one day /in a relative way";
    public static final String TEXT_4 = "and returned on the previous night.";

    @Before
    public void initSearcher() throws IOException {
        dir = newDirectory();
        w = new IndexWriter(dir, newIndexWriterConfig(new StandardAnalyzer()));
        Document d = new Document();
        d.add(new TextField(FIELD, TEXT_1, Field.Store.YES));
        d.add(new TextField("_uid", "1", Field.Store.YES));
        w.addDocument(d);
        d = new Document();
        d.add(new TextField(FIELD, TEXT_2, Field.Store.YES));
        d.add(new TextField("_uid", "2", Field.Store.YES));
        w.addDocument(d);
        d = new Document();
        d.add(new TextField(FIELD, TEXT_3, Field.Store.YES));
        d.add(new TextField("_uid", "3", Field.Store.YES));
        w.addDocument(d);
        d = new Document();
        d.add(new TextField(FIELD, TEXT_4, Field.Store.YES));
        d.add(new TextField("_uid", "4", Field.Store.YES));
        w.addDocument(d);
        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);
    }

    @After
    public void closeAllTheReaders() throws IOException {
        reader.close();
        w.close();
        dir.close();
    }

    public void testQueryFunctionFilterFunctionScore() throws IOException {
        FunctionScoreQueryBuilder functionScoreQueryBuilder = getQueryFunctionQueryBuilder();
        IndexMetaData.Builder indexMetadata = new IndexMetaData.Builder("index");
        indexMetadata.settings(Settings.builder().put("index.version.created", Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
        );
        IndexSettings indexSettings = new IndexSettings(indexMetadata.build(), Settings.EMPTY);
        MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        QueryShardContext context = new QueryShardContext(
            indexSettings, null, null, mapperService, null, null, null, null, null, null
        );
        Query query = functionScoreQueryBuilder.toQuery(context);
        TopDocs topDocs = searcher.search(query, 4);
        assertThat(topDocs.scoreDocs[0].score, equalTo(5f));
        assertThat(reader.document(topDocs.scoreDocs[0].doc).get("_uid"), equalTo("4"));
        assertThat(topDocs.scoreDocs[1].score, equalTo(4f));
        assertThat(reader.document(topDocs.scoreDocs[1].doc).get("_uid"), equalTo("1"));
        assertThat(topDocs.scoreDocs[2].score, equalTo(3f));
        assertThat(reader.document(topDocs.scoreDocs[2].doc).get("_uid"), equalTo("3"));
        assertThat(topDocs.scoreDocs[3].score, equalTo(2f));
        assertThat(reader.document(topDocs.scoreDocs[3].doc).get("_uid"), equalTo("2"));

        // now test explain
        Weight weight = searcher.createNormalizedWeight(query, true);
        Explanation explanation = weight.explain(searcher.getIndexReader().leaves().get(0), 0);
        assertThat(explanation.getDetails()[1].getDetails()[0].getDetails()[2].getDetails()[1].getDescription(),
            containsString("query_function score as computed by this query: function score (*:*, functions: [{filter(test:named), " +
                "function [org.elasticsearch.common.lucene.search.function.WeightFactorFunction"));

    }

    public void testQueryFunctionFunctionScore() throws IOException {
        FunctionScoreQueryBuilder functionScoreQueryBuilder = getQueryFunctionQueryBuilder();
        QueryShardContext context = getMockQueryShardContext();
        Query query = functionScoreQueryBuilder.toQuery(context);
        FunctionScoreQuery functionScoreQuery = new FunctionScoreQuery(new MatchAllDocsQuery(), new QueryFunction(query));
        TopDocs topDocs = searcher.search(functionScoreQuery, 4);
        assertThat(topDocs.scoreDocs[0].score, equalTo(5f));
        assertThat(reader.document(topDocs.scoreDocs[0].doc).get("_uid"), equalTo("4"));
        assertThat(topDocs.scoreDocs[1].score, equalTo(4f));
        assertThat(reader.document(topDocs.scoreDocs[1].doc).get("_uid"), equalTo("1"));
        assertThat(topDocs.scoreDocs[2].score, equalTo(3f));
        assertThat(reader.document(topDocs.scoreDocs[2].doc).get("_uid"), equalTo("3"));
        assertThat(topDocs.scoreDocs[3].score, equalTo(2f));
        assertThat(reader.document(topDocs.scoreDocs[3].doc).get("_uid"), equalTo("2"));
    }

    public static FunctionScoreQueryBuilder getQueryFunctionQueryBuilder() {
        FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders = new FunctionScoreQueryBuilder.FilterFunctionBuilder[4];
        String[] filterTerms = new String[]{"light", "relative", "named", "previous"};
        for (int i = 0; i < 4; i++) {
            FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(
                new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                    new FunctionScoreQueryBuilder.FilterFunctionBuilder(new TermQueryBuilder(FIELD, filterTerms[i]),
                        new WeightBuilder().setWeight(i + 2))});
            filterFunctionBuilders[i] = new FunctionScoreQueryBuilder.FilterFunctionBuilder(new QueryFunctionBuilder
                (functionScoreQueryBuilder));
        }
        return new FunctionScoreQueryBuilder(new MatchAllQueryBuilder(), filterFunctionBuilders);
    }
}
