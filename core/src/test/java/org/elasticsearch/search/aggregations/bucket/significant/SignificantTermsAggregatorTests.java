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

package org.elasticsearch.search.aggregations.bucket.significant;

import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;

public class SignificantTermsAggregatorTests extends AggregatorTestCase {

    private MappedFieldType fieldType;

    @Before
    public void setUpTest() throws Exception {
        super.setUp();
        fieldType = new KeywordFieldMapper.KeywordFieldType();
        fieldType.setHasDocValues(true);
        fieldType.setIndexOptions(IndexOptions.DOCS);
        fieldType.setName("field");
    }

    public void testParsedAsFilter() throws IOException {
        IndexReader indexReader = new MultiReader();
        IndexSearcher indexSearcher = newSearcher(indexReader);
        QueryBuilder filter = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("field", "foo"))
                .should(QueryBuilders.termQuery("field", "bar"));
        SignificantTermsAggregationBuilder builder = new SignificantTermsAggregationBuilder(
                "test", ValueType.STRING)
                .field("field")
                .backgroundFilter(filter);
        AggregatorFactory<?> factory = createAggregatorFactory(builder, indexSearcher, fieldType);
        assertThat(factory, Matchers.instanceOf(SignificantTermsAggregatorFactory.class));
        SignificantTermsAggregatorFactory sigTermsFactory =
                (SignificantTermsAggregatorFactory) factory;
        Query parsedQuery = sigTermsFactory.filter;
        assertThat(parsedQuery, Matchers.instanceOf(BooleanQuery.class));
        assertEquals(2, ((BooleanQuery) parsedQuery).clauses().size());
        // means the bool query has been parsed as a filter, if it was a query minShouldMatch would
        // be 0
        assertEquals(1, ((BooleanQuery) parsedQuery).getMinimumNumberShouldMatch());
    }

}
