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

package org.elasticsearch.join.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.InnerHitContextBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.fetch.subphase.InnerHitsContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.join.query.JoinQueryBuilders.hasParentQuery;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class HasParentQueryBuilderTests extends AbstractQueryTestCase<HasParentQueryBuilder> {
    private static final String TYPE = "doc";
    private static final String PARENT_DOC = "parent";
    private static final String CHILD_DOC = "child";

    boolean requiresRewrite = false;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(ParentJoinPlugin.class);
    }

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("doc").startObject("properties")
            .startObject("join_field")
                .field("type", "join")
                .startObject("relations")
                    .field(PARENT_DOC, CHILD_DOC)
                .endObject()
            .endObject()
            .startObject(STRING_FIELD_NAME)
                .field("type", "text")
            .endObject()
                .startObject(STRING_FIELD_NAME_2)
            .field("type", "keyword")
            .endObject()
            .startObject(INT_FIELD_NAME)
                .field("type", "integer")
            .endObject()
            .startObject(DOUBLE_FIELD_NAME)
                .field("type", "double")
            .endObject()
            .startObject(BOOLEAN_FIELD_NAME)
                .field("type", "boolean")
            .endObject()
            .startObject(DATE_FIELD_NAME)
                .field("type", "date")
            .endObject()
            .startObject(OBJECT_FIELD_NAME)
                .field("type", "object")
            .endObject()
            .endObject().endObject().endObject();

        mapperService.merge(TYPE,
            new CompressedXContent(mapping.string()), MapperService.MergeReason.MAPPING_UPDATE, false);
    }

    /**
     * @return a {@link HasChildQueryBuilder} with random values all over the place
     */
    @Override
    protected HasParentQueryBuilder doCreateTestQueryBuilder() {
        QueryBuilder innerQueryBuilder = new MatchAllQueryBuilder();
        if (randomBoolean()) {
            requiresRewrite = true;
            innerQueryBuilder = new WrapperQueryBuilder(innerQueryBuilder.toString());
        }
        HasParentQueryBuilder hqb = new HasParentQueryBuilder(PARENT_DOC, innerQueryBuilder, randomBoolean());
        hqb.ignoreUnmapped(randomBoolean());
        if (randomBoolean()) {
            hqb.innerHit(new InnerHitBuilder()
                    .setName(randomAlphaOfLengthBetween(1, 10))
                    .setSize(randomIntBetween(0, 100))
                    .addSort(new FieldSortBuilder(STRING_FIELD_NAME_2).order(SortOrder.ASC))
                    .setIgnoreUnmapped(hqb.ignoreUnmapped()));
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
            Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(queryBuilder, innerHitBuilders);
            for (InnerHitContextBuilder builder : innerHitBuilders.values()) {
                builder.build(searchContext, searchContext.innerHits());
            }
            assertNotNull(searchContext.innerHits());
            assertEquals(1, searchContext.innerHits().getInnerHits().size());
            assertTrue(searchContext.innerHits().getInnerHits().containsKey(queryBuilder.innerHit().getName()));
            InnerHitsContext.InnerHitSubContext innerHits = searchContext.innerHits()
                    .getInnerHits().get(queryBuilder.innerHit().getName());
            assertEquals(innerHits.size(), queryBuilder.innerHit().getSize());
            assertEquals(innerHits.sort().sort.getSort().length, 1);
            assertEquals(innerHits.sort().sort.getSort()[0].getField(), STRING_FIELD_NAME_2);
        }
    }

    /**
     * Test (de)serialization on all previous released versions
     */
    public void testSerializationBWC() throws IOException {
        for (Version version : VersionUtils.allReleasedVersions()) {
            HasParentQueryBuilder testQuery = createTestQueryBuilder();
            if (version.before(Version.V_5_2_0) && testQuery.innerHit() != null) {
                // ignore unmapped for inner_hits has been added on 5.2
                testQuery.innerHit().setIgnoreUnmapped(false);
            }
            assertSerialization(testQuery, version);
        }
    }

    public void testIllegalValues() throws IOException {
        QueryBuilder query = new MatchAllQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> hasParentQuery(null, query, false));
        assertThat(e.getMessage(), equalTo("[has_parent] requires 'type' field"));

        e = expectThrows(IllegalArgumentException.class,
                () -> hasParentQuery("foo", null, false));
        assertThat(e.getMessage(), equalTo("[has_parent] requires 'query' field"));

        QueryShardContext context = createShardContext();
        HasParentQueryBuilder qb = hasParentQuery("just_a_type", new MatchAllQueryBuilder(), false);
        QueryShardException qse = expectThrows(QueryShardException.class, () -> qb.doToQuery(context));
        assertThat(qse.getMessage(), equalTo("[has_parent] join field [join_field] doesn't hold [just_a_type] as a parent"));
    }

    public void testToQueryInnerQueryType() throws IOException {
        String[] searchTypes = new String[]{TYPE};
        QueryShardContext shardContext = createShardContext();
        shardContext.setTypes(searchTypes);
        HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder(PARENT_DOC, new IdsQueryBuilder().addIds("id"),
                false);
        Query query = hasParentQueryBuilder.toQuery(shardContext);
        //verify that the context types are still the same as the ones we previously set
        assertThat(shardContext.getTypes(), equalTo(searchTypes));
        HasChildQueryBuilderTests.assertLateParsingQuery(query, PARENT_DOC, "id");
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
                    containsString("[has_parent] join field [join_field] doesn't hold [unmapped] as a parent"));
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
