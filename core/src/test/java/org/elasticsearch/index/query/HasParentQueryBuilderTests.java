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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class HasParentQueryBuilderTests extends AbstractQueryTestCase<HasParentQueryBuilder> {
    protected static final String PARENT_TYPE = "parent";
    protected static final String CHILD_TYPE = "child";

    boolean requiresRewrite = false;

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
        QueryBuilder innerQueryBuilder = RandomQueryBuilder.createQuery(random());
        if (randomBoolean()) {
            requiresRewrite = true;
            innerQueryBuilder = new WrapperQueryBuilder(innerQueryBuilder.toString());
        }
        HasParentQueryBuilder hqb = new HasParentQueryBuilder(PARENT_TYPE, innerQueryBuilder, randomBoolean());
        hqb.ignoreUnmapped(randomBoolean());
        if (randomBoolean()) {
            hqb.innerHit(new InnerHitBuilder()
                    .setName(randomAsciiOfLengthBetween(1, 10))
                    .setSize(randomIntBetween(0, 100))
                    .addSort(new FieldSortBuilder(STRING_FIELD_NAME_2).order(SortOrder.ASC)), hqb.ignoreUnmapped());
        }
        return hqb;
    }

    @Override
    protected void doAssertLuceneQuery(HasParentQueryBuilder queryBuilder, Query query, SearchContext searchContext) throws IOException {
        assertThat(query, instanceOf(HasChildQueryBuilder.LateParsingQuery.class));
        HasChildQueryBuilder.LateParsingQuery lpq = (HasChildQueryBuilder.LateParsingQuery) query;
        assertEquals(queryBuilder.score() ? ScoreMode.Max : ScoreMode.None, lpq.getScoreMode());

        if (queryBuilder.innerHit() != null) {
            // have to rewrite again because the provided queryBuilder hasn't been rewritten (directly returned from
            // doCreateTestQueryBuilder)
            queryBuilder = (HasParentQueryBuilder) queryBuilder.rewrite(searchContext.getQueryShardContext());

            assertNotNull(searchContext);
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
        HasParentQueryBuilder queryBuilder = (HasParentQueryBuilder) parseQuery(builder.string());
        assertEquals("foo", queryBuilder.type());
        assertWarnings("Deprecated field [type] used, expected [parent_type] instead");
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

    @Override
    public void testMustRewrite() throws IOException {
        try {
            super.testMustRewrite();
        } catch (UnsupportedOperationException e) {
            if (requiresRewrite == false) {
                throw e;
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

    public void testIgnoreUnmappedWithRewrite() throws IOException {
        // WrapperQueryBuilder makes sure we always rewrite
        final HasParentQueryBuilder queryBuilder =
            new HasParentQueryBuilder("unmapped", new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()), false);
        queryBuilder.ignoreUnmapped(true);
        QueryShardContext queryShardContext = createShardContext();
        Query query = queryBuilder.rewrite(queryShardContext).toQuery(queryShardContext);
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }
}
