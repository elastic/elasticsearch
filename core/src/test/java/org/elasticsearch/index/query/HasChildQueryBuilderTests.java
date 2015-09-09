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

package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.support.QueryInnerHits;
import org.elasticsearch.search.fetch.innerhits.InnerHitsBuilder;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.CoreMatchers.instanceOf;

public class HasChildQueryBuilderTests extends BaseQueryTestCase<HasChildQueryBuilder> {
    protected static final String PARENT_TYPE = "parent";
    protected static final String CHILD_TYPE = "child";

    public void setUp() throws Exception {
        super.setUp();
        MapperService mapperService = queryParserService().mapperService;
        mapperService.merge(PARENT_TYPE, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(PARENT_TYPE,
                STRING_FIELD_NAME, "type=string",
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object"
        ).string()), false, false);
        mapperService.merge(CHILD_TYPE, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(CHILD_TYPE,
                "_parent", "type=" + PARENT_TYPE,
                STRING_FIELD_NAME, "type=string",
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object"
        ).string()), false, false);
    }

    protected void setSearchContext(String[] types) {
        final MapperService mapperService = queryParserService().mapperService;
        final IndexFieldDataService fieldData = queryParserService().fieldDataService;
        TestSearchContext testSearchContext = new TestSearchContext() {
            private InnerHitsContext context;


            @Override
            public void innerHits(InnerHitsContext innerHitsContext) {
                context = innerHitsContext;
            }

            @Override
            public InnerHitsContext innerHits() {
                return context;
            }

            @Override
            public MapperService mapperService() {
                return mapperService; // need to build / parse inner hits sort fields
            }

            @Override
            public IndexFieldDataService fieldData() {
                return fieldData; // need to build / parse inner hits sort fields
            }
        };
        testSearchContext.setTypes(types);
        SearchContext.setCurrent(testSearchContext);
    }

    /**
     * @return a {@link HasChildQueryBuilder} with random values all over the place
     */
    @Override
    protected HasChildQueryBuilder doCreateTestQueryBuilder() {
        int min = randomIntBetween(0, Integer.MAX_VALUE / 2);
        int max = randomIntBetween(min, Integer.MAX_VALUE);
        InnerHitsBuilder.InnerHit innerHit = new InnerHitsBuilder.InnerHit().setSize(100).addSort(STRING_FIELD_NAME, SortOrder.ASC);
        return new HasChildQueryBuilder(CHILD_TYPE,
                RandomQueryBuilder.createQuery(random()), max, min,
                RandomPicks.randomFrom(random(), ScoreType.values()),
                SearchContext.current() == null ? null : new QueryInnerHits("inner_hits_name", innerHit));
    }

    @Override
    protected void doAssertLuceneQuery(HasChildQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        QueryBuilder innerQueryBuilder = queryBuilder.query();
        if (innerQueryBuilder instanceof EmptyQueryBuilder) {
            assertNull(query);
        } else {
            assertThat(query, instanceOf(HasChildQueryBuilder.LateParsingQuery.class));
            HasChildQueryBuilder.LateParsingQuery lpq = (HasChildQueryBuilder.LateParsingQuery) query;
            assertEquals(queryBuilder.minChildren(), lpq.getMinChildren());
            assertEquals(queryBuilder.maxChildren(), lpq.getMaxChildren());
            assertEquals(HasChildQueryBuilder.scoreTypeToScoreMode(queryBuilder.scoreType()), lpq.getScoreMode()); // WTF is this why do we have two?
        }
        if (queryBuilder.innerHit() != null) {
            assertNotNull(SearchContext.current());
            if (query != null) {
                assertNotNull(SearchContext.current().innerHits());
                assertEquals(1, SearchContext.current().innerHits().getInnerHits().size());
                assertTrue(SearchContext.current().innerHits().getInnerHits().containsKey("inner_hits_name"));
                InnerHitsContext.BaseInnerHits innerHits = SearchContext.current().innerHits().getInnerHits().get("inner_hits_name");
                assertEquals(innerHits.size(), 100);
                assertEquals(innerHits.sort().getSort().length, 1);
                assertEquals(innerHits.sort().getSort()[0].getField(), STRING_FIELD_NAME);
            } else {
                assertNull(SearchContext.current().innerHits());
            }
        }
    }

    public void testIllegalValues() {
        QueryBuilder query = RandomQueryBuilder.createQuery(random());
        try {
            new HasChildQueryBuilder(null, query);
            fail("must not be null");
        } catch (IllegalArgumentException ex) {

        }

        try {
            new HasChildQueryBuilder("foo", null);
            fail("must not be null");
        } catch (IllegalArgumentException ex) {

        }
        HasChildQueryBuilder foo = new HasChildQueryBuilder("foo", query);// all good
        try {
            foo.scoreType(null);
            fail("must not be null");
        } catch (IllegalArgumentException ex) {

        }
        final int positiveValue = randomIntBetween(0, Integer.MAX_VALUE);
        try {
            foo.minChildren(randomIntBetween(Integer.MIN_VALUE, -1));
            fail("must not be negative");
        } catch (IllegalArgumentException ex) {

        }
        foo.minChildren(positiveValue);
        assertEquals(positiveValue, foo.minChildren());
        try {
            foo.maxChildren(randomIntBetween(Integer.MIN_VALUE, -1));
            fail("must not be negative");
        } catch (IllegalArgumentException ex) {

        }

        foo.maxChildren(positiveValue);
        assertEquals(positiveValue, foo.maxChildren());
    }

    public void testParseFromJSON() throws IOException {
        String query = copyToStringFromClasspath("/org/elasticsearch/index/query/has-child-with-inner-hits.json").trim();
        HasChildQueryBuilder queryBuilder = (HasChildQueryBuilder) parseQuery(query);
        assertEquals(query, queryBuilder.maxChildren(), 1217235442);
        assertEquals(query, queryBuilder.minChildren(), 883170873);
        assertEquals(query, queryBuilder.boost(), 2.0f, 0.0f);
        assertEquals(query, queryBuilder.queryName(), "WNzYMJKRwePuRBh");
        assertEquals(query, queryBuilder.childType(), "child");
        assertEquals(query, queryBuilder.scoreType(), ScoreType.AVG);
        assertNotNull(query, queryBuilder.innerHit());
        assertEquals(query, queryBuilder.innerHit(), new QueryInnerHits("inner_hits_name", new InnerHitsBuilder.InnerHit().setSize(100).addSort("mapped_string", SortOrder.ASC)));
        // now assert that we actually generate the same JSON
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        queryBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(query, builder.string());
    }

}
