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

import com.fasterxml.jackson.core.JsonParseException;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.script.Script.ScriptParseException;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.startsWith;

public class HasParentQueryBuilderTests extends AbstractQueryTestCase<HasParentQueryBuilder> {
    protected static final String PARENT_TYPE = "parent";
    protected static final String CHILD_TYPE = "child";

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(PARENT_TYPE, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(PARENT_TYPE,
                STRING_FIELD_NAME, "type=text",
                STRING_FIELD_NAME_2, "type=keyword",
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mapperService.merge(CHILD_TYPE, new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef(CHILD_TYPE,
                "_parent", "type=" + PARENT_TYPE,
                STRING_FIELD_NAME, "type=text",
                STRING_FIELD_NAME_2, "type=keyword",
                INT_FIELD_NAME, "type=integer",
                DOUBLE_FIELD_NAME, "type=double",
                BOOLEAN_FIELD_NAME, "type=boolean",
                DATE_FIELD_NAME, "type=date",
                OBJECT_FIELD_NAME, "type=object"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
        mapperService.merge("just_a_type", new CompressedXContent(PutMappingRequest.buildFromSimplifiedDef("just_a_type"
        ).string()), MapperService.MergeReason.MAPPING_UPDATE, false);
    }

    /**
     * @return a {@link HasChildQueryBuilder} with random values all over the place
     */
    @Override
    protected HasParentQueryBuilder doCreateTestQueryBuilder() {
        HasParentQueryBuilder hqb = new HasParentQueryBuilder(PARENT_TYPE,
                RandomQueryBuilder.createQuery(random()),randomBoolean());
        if (randomBoolean()) {
            hqb.innerHit(new InnerHitBuilder()
                    .setName(randomAsciiOfLengthBetween(1, 10))
                    .setSize(randomIntBetween(0, 100))
                    .addSort(new FieldSortBuilder(STRING_FIELD_NAME_2).order(SortOrder.ASC)));
        }
        hqb.ignoreUnmapped(randomBoolean());
        return hqb;
    }

    @Override
    protected void doAssertLuceneQuery(HasParentQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(HasChildQueryBuilder.LateParsingQuery.class));
        HasChildQueryBuilder.LateParsingQuery lpq = (HasChildQueryBuilder.LateParsingQuery) query;
        assertEquals(queryBuilder.score() ? ScoreMode.Max : ScoreMode.None, lpq.getScoreMode());

        if (queryBuilder.innerHit() != null) {
            SearchContext searchContext = SearchContext.current();
            assertNotNull(searchContext);
            if (query != null) {
                Map<String, InnerHitBuilder> innerHitBuilders = new HashMap<>();
                InnerHitBuilder.extractInnerHits(queryBuilder, innerHitBuilders);
                for (InnerHitBuilder builder : innerHitBuilders.values()) {
                    builder.build(searchContext, searchContext.innerHits());
                }
                assertNotNull(searchContext.innerHits());
                assertEquals(1, searchContext.innerHits().getInnerHits().size());
                assertTrue(searchContext.innerHits().getInnerHits().containsKey(queryBuilder.innerHit().getName()));
                InnerHitsContext.BaseInnerHits innerHits = searchContext.innerHits()
                        .getInnerHits().get(queryBuilder.innerHit().getName());
                assertEquals(innerHits.size(), queryBuilder.innerHit().getSize());
                assertEquals(innerHits.sort().sort.getSort().length, 1);
                assertEquals(innerHits.sort().sort.getSort()[0].getField(), STRING_FIELD_NAME_2);
            } else {
                assertThat(searchContext.innerHits().getInnerHits().size(), equalTo(0));
            }
        }
    }

    public void testIllegalValues() throws IOException {
        QueryBuilder query = RandomQueryBuilder.createQuery(random());
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> QueryBuilders.hasParentQuery(null, query, false));
        assertThat(e.getMessage(), equalTo("[has_parent] requires 'type' field"));

        e = expectThrows(IllegalArgumentException.class,
                () -> QueryBuilders.hasParentQuery("foo", null, false));
        assertThat(e.getMessage(), equalTo("[has_parent] requires 'query' field"));

        QueryShardContext context = createShardContext();
        HasParentQueryBuilder qb = QueryBuilders.hasParentQuery("just_a_type", new MatchAllQueryBuilder(), false);
        QueryShardException qse = expectThrows(QueryShardException.class, () -> qb.doToQuery(context));
        assertThat(qse.getMessage(), equalTo("[has_parent] no child types found for type [just_a_type]"));
    }

    public void testDeprecatedXContent() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
        builder.startObject();
        builder.startObject("has_parent");
        builder.field("query");
        new TermQueryBuilder("a", "a").toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.field("type", "foo"); // deprecated
        builder.endObject();
        builder.endObject();
        try {
            parseQuery(builder.string());
            fail("type is deprecated");
        } catch (IllegalArgumentException ex) {
            assertEquals("Deprecated field [type] used, expected [parent_type] instead", ex.getMessage());
        }

        HasParentQueryBuilder queryBuilder = (HasParentQueryBuilder) parseQuery(builder.string(), ParseFieldMatcher.EMPTY);
        assertEquals("foo", queryBuilder.type());
    }

    public void testToQueryInnerQueryType() throws IOException {
        String[] searchTypes = new String[]{CHILD_TYPE};
        QueryShardContext shardContext = createShardContext();
        shardContext.setTypes(searchTypes);
        HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder(PARENT_TYPE, new IdsQueryBuilder().addIds("id"),
                false);
        Query query = hasParentQueryBuilder.toQuery(shardContext);
        //verify that the context types are still the same as the ones we previously set
        assertThat(shardContext.getTypes(), equalTo(searchTypes));
        HasChildQueryBuilderTests.assertLateParsingQuery(query, PARENT_TYPE, "id");
    }

    /**
     * override superclass test, because here we need to take care that mutation doesn't happen inside
     * `inner_hits` structure, because we don't parse them yet and so no exception will be triggered
     * for any mutation there.
     */
    @Override
    public void testUnknownObjectException() throws IOException {
        String validQuery = createTestQueryBuilder().toString();
        assertThat(validQuery, containsString("{"));
        int endPosition = validQuery.indexOf("inner_hits");
        if (endPosition == -1) {
            endPosition = validQuery.length() - 1;
        }
        for (int insertionPosition = 0; insertionPosition < endPosition; insertionPosition++) {
            if (validQuery.charAt(insertionPosition) == '{') {
                String testQuery = validQuery.substring(0, insertionPosition) + "{ \"newField\" : " + validQuery.substring(insertionPosition) + "}";
                try {
                    parseQuery(testQuery);
                    fail("some parsing exception expected for query: " + testQuery);
                } catch (ParsingException | ScriptParseException | ElasticsearchParseException e) {
                    // different kinds of exception wordings depending on location
                    // of mutation, so no simple asserts possible here
                } catch (JsonParseException e) {
                    // mutation produced invalid json
                }
            }
        }
    }

    public void testFromJson() throws IOException {
        String json =
                "{\n" +
                "  \"has_parent\" : {\n" +
                "    \"query\" : {\n" +
                "      \"term\" : {\n" +
                "        \"tag\" : {\n" +
                "          \"value\" : \"something\",\n" +
                "          \"boost\" : 1.0\n" +
                "        }\n" +
                "      }\n" +
                "    },\n" +
                "    \"parent_type\" : \"blog\",\n" +
                "    \"score\" : true,\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 1.0\n" +
                "  }\n" +
                "}";
        HasParentQueryBuilder parsed = (HasParentQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals(json, "blog", parsed.type());
        assertEquals(json, "something", ((TermQueryBuilder) parsed.query()).value());
    }

    /**
     * we resolve empty inner clauses by representing this whole query as empty optional upstream
     */
    public void testFromJsonEmptyQueryBody() throws IOException {
        String query =  "{\n" +
                "  \"has_parent\" : {\n" +
                "    \"query\" : { },\n" +
                "    \"parent_type\" : \"blog\"" +
                "   }" +
                "}";
        XContentParser parser = XContentFactory.xContent(query).createParser(query);
        QueryParseContext context = createParseContext(parser, ParseFieldMatcher.EMPTY);
        Optional<QueryBuilder> innerQueryBuilder = context.parseInnerQueryBuilder();
        assertTrue(innerQueryBuilder.isPresent() == false);

        parser = XContentFactory.xContent(query).createParser(query);
        QueryParseContext otherContext = createParseContext(parser, ParseFieldMatcher.STRICT);
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> otherContext.parseInnerQueryBuilder());
        assertThat(ex.getMessage(), startsWith("query malformed, empty clause found at"));
    }

    public void testIgnoreUnmapped() throws IOException {
        final HasParentQueryBuilder queryBuilder = new HasParentQueryBuilder("unmapped", new MatchAllQueryBuilder(), false);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final HasParentQueryBuilder failingQueryBuilder = new HasParentQueryBuilder("unmapped", new MatchAllQueryBuilder(), false);
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(),
                    containsString("[" + HasParentQueryBuilder.NAME + "] query configured 'parent_type' [unmapped] is not a valid type"));
    }
}
