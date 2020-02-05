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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.join.ParentJoinPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;

public class ParentIdQueryBuilderTests extends AbstractQueryTestCase<ParentIdQueryBuilder> {

    private static final String TYPE = "_doc";
    private static final String JOIN_FIELD_NAME = "join_field";
    private static final String PARENT_NAME = "parent";
    private static final String CHILD_NAME = "child";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(ParentJoinPlugin.class);
    }

    @Override
    protected Settings createTestIndexSettings() {
        return Settings.builder()
            .put(super.createTestIndexSettings())
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject().startObject("_doc").startObject("properties")
            .startObject("join_field")
                .field("type", "join")
                .startObject("relations")
                    .field("parent", "child")
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
            new CompressedXContent(Strings.toString(mapping)), MapperService.MergeReason.MAPPING_UPDATE);
    }

    @Override
    protected ParentIdQueryBuilder doCreateTestQueryBuilder() {
        return new ParentIdQueryBuilder(CHILD_NAME, randomAlphaOfLength(4)).ignoreUnmapped(randomBoolean());
    }

    @Override
    protected void doAssertLuceneQuery(ParentIdQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, Matchers.instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.clauses().size(), Matchers.equalTo(2));
        BooleanQuery expected = new BooleanQuery.Builder()
            .add(new TermQuery(new Term(JOIN_FIELD_NAME + "#" + PARENT_NAME, queryBuilder.getId())), BooleanClause.Occur.MUST)
            .add(new TermQuery(new Term(JOIN_FIELD_NAME, queryBuilder.getType())), BooleanClause.Occur.FILTER)
            .build();
        assertThat(expected, equalTo(query));
    }

    public void testFromJson() throws IOException {
        String query =
            "{\n" +
                "  \"parent_id\" : {\n" +
                "    \"type\" : \"child\",\n" +
                "    \"id\" : \"123\",\n" +
                "    \"ignore_unmapped\" : false,\n" +
                "    \"boost\" : 3.0,\n" +
                "    \"_name\" : \"name\"" +
                "  }\n" +
                "}";
        ParentIdQueryBuilder queryBuilder = (ParentIdQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);
        assertThat(queryBuilder.getType(), Matchers.equalTo("child"));
        assertThat(queryBuilder.getId(), Matchers.equalTo("123"));
        assertThat(queryBuilder.boost(), Matchers.equalTo(3f));
        assertThat(queryBuilder.queryName(), Matchers.equalTo("name"));
    }

    public void testIgnoreUnmapped() throws IOException {
        final ParentIdQueryBuilder queryBuilder = new ParentIdQueryBuilder("unmapped", "foo");
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final ParentIdQueryBuilder failingQueryBuilder = new ParentIdQueryBuilder("unmapped", "foo");
        failingQueryBuilder.ignoreUnmapped(false);
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("[" + ParentIdQueryBuilder.NAME + "] no relation found for child [unmapped]"));
    }

}
