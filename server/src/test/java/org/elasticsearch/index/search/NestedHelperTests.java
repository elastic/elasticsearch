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

package org.elasticsearch.index.search;

import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.SearchFields;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Collections;

public class NestedHelperTests extends ESSingleNodeTestCase {

    private IndexService indexService;
    private SearchFields searchFields;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("_doc")
                    .startObject("properties")
                        .startObject("foo")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("foo2")
                            .field("type", "long")
                        .endObject()
                        .startObject("nested1")
                            .field("type", "nested")
                            .startObject("properties")
                                .startObject("foo")
                                    .field("type", "keyword")
                                .endObject()
                                .startObject("foo2")
                                    .field("type", "long")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("nested2")
                            .field("type", "nested")
                            .field("include_in_parent", true)
                            .startObject("properties")
                                .startObject("foo")
                                    .field("type", "keyword")
                                .endObject()
                                .startObject("foo2")
                                    .field("type", "long")
                                .endObject()
                            .endObject()
                        .endObject()
                        .startObject("nested3")
                            .field("type", "nested")
                            .field("include_in_root", true)
                            .startObject("properties")
                                .startObject("foo")
                                    .field("type", "keyword")
                                .endObject()
                                .startObject("foo2")
                                    .field("type", "long")
                                .endObject()
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject().endObject();
        indexService = createIndex("index", Settings.EMPTY, mapping);
        searchFields = new SearchFields(indexService.mapperService());
    }

    private static NestedHelper buildNestedHelper(SearchFields searchFields) {
        return new NestedHelper(searchFields);
    }

    public void testMatchAll() {
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(new MatchAllDocsQuery()));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(new MatchAllDocsQuery(), "nested_missing"));
    }

    public void testMatchNo() {
        assertFalse(buildNestedHelper(searchFields).mightMatchNestedDocs(new MatchNoDocsQuery()));
        assertFalse(buildNestedHelper(searchFields).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested1"));
        assertFalse(buildNestedHelper(searchFields).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested2"));
        assertFalse(buildNestedHelper(searchFields).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested3"));
        assertFalse(buildNestedHelper(searchFields).mightMatchNonNestedDocs(new MatchNoDocsQuery(), "nested_missing"));
    }

    public void testTermsQuery() {
        Query termsQuery = searchFields.fieldType("foo").termsQuery(Collections.singletonList("bar"), null);
        assertFalse(buildNestedHelper(searchFields).mightMatchNestedDocs(termsQuery));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested_missing"));

        termsQuery = searchFields.fieldType("nested1.foo").termsQuery(Collections.singletonList("bar"), null);
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(termsQuery));
        assertFalse(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested_missing"));

        termsQuery = searchFields.fieldType("nested2.foo").termsQuery(Collections.singletonList("bar"), null);
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(termsQuery));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested_missing"));

        termsQuery = searchFields.fieldType("nested3.foo").termsQuery(Collections.singletonList("bar"), null);
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(termsQuery));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termsQuery, "nested_missing"));
    }

    public void testTermQuery() {
        Query termQuery = searchFields.fieldType("foo").termQuery("bar", null);
        assertFalse(buildNestedHelper(searchFields).mightMatchNestedDocs(termQuery));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested_missing"));

        termQuery = searchFields.fieldType("nested1.foo").termQuery("bar", null);
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(termQuery));
        assertFalse(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested_missing"));

        termQuery = searchFields.fieldType("nested2.foo").termQuery("bar", null);
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(termQuery));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested_missing"));

        termQuery = searchFields.fieldType("nested3.foo").termQuery("bar", null);
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(termQuery));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(termQuery, "nested_missing"));
    }

    public void testRangeQuery() {
        QueryShardContext context = createSearchContext(indexService).getQueryShardContext();
        Query rangeQuery = searchFields.fieldType("foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertFalse(buildNestedHelper(searchFields).mightMatchNestedDocs(rangeQuery));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));

        rangeQuery = searchFields.fieldType("nested1.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(rangeQuery));
        assertFalse(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));

        rangeQuery = searchFields.fieldType("nested2.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(rangeQuery));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));

        rangeQuery = searchFields.fieldType("nested3.foo2").rangeQuery(2, 5, true, true, null, null, null, context);
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(rangeQuery));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(rangeQuery, "nested_missing"));
    }

    public void testDisjunction() {
        BooleanQuery bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("foo", "baz")), Occur.SHOULD)
                .build();
        assertFalse(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested1.foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("nested1.foo", "baz")), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertFalse(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested2.foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("nested2.foo", "baz")), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested3.foo", "bar")), Occur.SHOULD)
                .add(new TermQuery(new Term("nested3.foo", "baz")), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested3"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("foo", "bar")), Occur.SHOULD)
                .add(new MatchAllDocsQuery(), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested1.foo", "bar")), Occur.SHOULD)
                .add(new MatchAllDocsQuery(), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested2.foo", "bar")), Occur.SHOULD)
                .add(new MatchAllDocsQuery(), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested3.foo", "bar")), Occur.SHOULD)
                .add(new MatchAllDocsQuery(), Occur.SHOULD)
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested3"));
    }

    private static Occur requiredOccur() {
        return random().nextBoolean() ? Occur.MUST : Occur.FILTER;
    }

    public void testConjunction() {
        BooleanQuery bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("foo", "bar")), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertFalse(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested1.foo", "bar")), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertFalse(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested2.foo", "bar")), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested3.foo", "bar")), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested3"));

        bq = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested1"));

        bq = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested2"));

        bq = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), requiredOccur())
                .add(new MatchAllDocsQuery(), requiredOccur())
                .build();
        assertTrue(buildNestedHelper(searchFields).mightMatchNestedDocs(bq));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(bq, "nested3"));
    }

    public void testNested() throws IOException {
        QueryShardContext context = indexService.newQueryShardContext(0, new IndexSearcher(new MultiReader()), () -> 0, null);
        NestedQueryBuilder queryBuilder = new NestedQueryBuilder("nested1", new MatchAllQueryBuilder(), ScoreMode.Avg);
        ESToParentBlockJoinQuery query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        Query expectedChildQuery = new BooleanQuery.Builder()
                .add(new MatchAllDocsQuery(), Occur.MUST)
                // we automatically add a filter since the inner query might match non-nested docs
                .add(new TermQuery(new Term("_nested_path", "nested1")), Occur.FILTER)
                .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(buildNestedHelper(searchFields).mightMatchNestedDocs(query));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested_missing"));

        queryBuilder = new NestedQueryBuilder("nested1", new TermQueryBuilder("nested1.foo", "bar"), ScoreMode.Avg);
        query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // this time we do not add a filter since the inner query only matches inner docs
        expectedChildQuery = new TermQuery(new Term("nested1.foo", "bar"));
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(buildNestedHelper(searchFields).mightMatchNestedDocs(query));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested_missing"));

        queryBuilder = new NestedQueryBuilder("nested2", new TermQueryBuilder("nested2.foo", "bar"), ScoreMode.Avg);
        query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // we need to add the filter again because of include_in_parent
        expectedChildQuery = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested2.foo", "bar")), Occur.MUST)
                .add(new TermQuery(new Term("_nested_path", "nested2")), Occur.FILTER)
                .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(buildNestedHelper(searchFields).mightMatchNestedDocs(query));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested_missing"));

        queryBuilder = new NestedQueryBuilder("nested3", new TermQueryBuilder("nested3.foo", "bar"), ScoreMode.Avg);
        query = (ESToParentBlockJoinQuery) queryBuilder.toQuery(context);

        // we need to add the filter again because of include_in_root
        expectedChildQuery = new BooleanQuery.Builder()
                .add(new TermQuery(new Term("nested3.foo", "bar")), Occur.MUST)
                .add(new TermQuery(new Term("_nested_path", "nested3")), Occur.FILTER)
                .build();
        assertEquals(expectedChildQuery, query.getChildQuery());

        assertFalse(buildNestedHelper(searchFields).mightMatchNestedDocs(query));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested1"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested2"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested3"));
        assertTrue(buildNestedHelper(searchFields).mightMatchNonNestedDocs(query, "nested_missing"));
    }
}
