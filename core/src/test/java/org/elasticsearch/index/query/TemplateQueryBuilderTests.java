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

import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script.ScriptParseException;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.script.Template;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TemplateQueryBuilderTests extends AbstractQueryTestCase<TemplateQueryBuilder> {

    /**
     * The query type all template tests will be based on.
     */
    private QueryBuilder templateBase;

    @Before
    public void before() {
        templateBase = RandomQueryBuilder.createQuery(random());
    }

    @Override
    protected boolean supportsBoostAndQueryName() {
        return false;
    }

    @Override
    protected TemplateQueryBuilder doCreateTestQueryBuilder() {
        return new TemplateQueryBuilder(new Template(templateBase.toString(), ScriptType.INLINE, "mockscript", null, null));
    }

    @Override
    protected void doAssertLuceneQuery(TemplateQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertEquals(rewrite(QueryBuilder.rewriteQuery(templateBase, context).toQuery(context)), rewrite(query));
    }

    public void testIllegalArgument() {
        expectThrows(IllegalArgumentException.class, () -> new TemplateQueryBuilder((Template) null));
    }

    /**
     * Override superclass test since template query doesn't support boost and queryName, so
     * we need to mutate other existing field in the test query.
     */
    @Override
    public void testUnknownField() throws IOException {
        TemplateQueryBuilder testQuery = createTestQueryBuilder();
        String testQueryAsString = toXContent(testQuery, randomFrom(XContentType.JSON, XContentType.YAML)).string();
        String queryAsString = testQueryAsString.replace("inline", "bogusField");
        try {
            parseQuery(queryAsString);
            fail("ScriptParseException expected.");
        } catch (ScriptParseException e) {
            assertTrue(e.getMessage().contains("bogusField"));
        }
    }

    public void testJSONGeneration() throws IOException {
        Map<String, Object> vars = new HashMap<>();
        vars.put("template", "filled");
        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                new Template("I am a $template string", ScriptType.INLINE, null, null, vars));
        XContentBuilder content = XContentFactory.jsonBuilder();
        content.startObject();
        builder.doXContent(content, null);
        content.endObject();
        content.close();
        assertEquals("{\"template\":{\"inline\":\"I am a $template string\",\"lang\":\"mustache\",\"params\":{\"template\":\"filled\"}}}",
                content.string());
    }

    public void testRawEscapedTemplate() throws IOException {
        String expectedTemplateString = "{\"match_{{template}}\": {}}\"";
        String query = "{\"template\": {\"query\": \"{\\\"match_{{template}}\\\": {}}\\\"\",\"params\" : {\"template\" : \"all\"}}}";
        Map<String, Object> params = new HashMap<>();
        params.put("template", "all");
        QueryBuilder expectedBuilder = new TemplateQueryBuilder(new Template(expectedTemplateString, ScriptType.INLINE, null, null,
                params));
        assertParsedQuery(query, expectedBuilder);
    }

    public void testRawTemplate() throws IOException {
        String expectedTemplateString = "{\"match_{{template}}\":{}}";
        String query = "{\"template\": {\"query\": {\"match_{{template}}\": {}},\"params\" : {\"template\" : \"all\"}}}";
        Map<String, Object> params = new HashMap<>();
        params.put("template", "all");
        QueryBuilder expectedBuilder = new TemplateQueryBuilder(new Template(expectedTemplateString, ScriptType.INLINE, null,
                XContentType.JSON, params));
        assertParsedQuery(query, expectedBuilder);
    }

    @Override
    public void testMustRewrite() throws IOException {
        String query = "{ \"match_all\" : {}}";
        QueryBuilder builder = new TemplateQueryBuilder(new Template(query, ScriptType.INLINE, "mockscript",
        XContentType.JSON, Collections.emptyMap()));
        try {
            builder.toQuery(createShardContext());
            fail();
        } catch (UnsupportedOperationException ex) {
            assertEquals("this query must be rewritten first", ex.getMessage());
        }
        assertEquals(new MatchAllQueryBuilder(), builder.rewrite(createShardContext()));
    }

    public void testRewriteWithInnerName() throws IOException {
        final String query = "{ \"match_all\" : {\"_name\" : \"foobar\"}}";
        QueryBuilder builder = new TemplateQueryBuilder(new Template(query, ScriptType.INLINE, "mockscript",
            XContentType.JSON, Collections.emptyMap()));
        assertEquals(new MatchAllQueryBuilder().queryName("foobar"), builder.rewrite(createShardContext()));

        builder = new TemplateQueryBuilder(new Template(query, ScriptType.INLINE, "mockscript",
            XContentType.JSON, Collections.emptyMap())).queryName("outer");
        assertEquals(new BoolQueryBuilder().must(new MatchAllQueryBuilder().queryName("foobar")).queryName("outer"),
            builder.rewrite(createShardContext()));
    }

    public void testRewriteWithInnerBoost() throws IOException {
        final TermQueryBuilder query = new TermQueryBuilder("foo", "bar").boost(2);
        QueryBuilder builder = new TemplateQueryBuilder(new Template(query.toString(), ScriptType.INLINE, "mockscript",
            XContentType.JSON, Collections.emptyMap()));
        assertEquals(query, builder.rewrite(createShardContext()));

        builder = new TemplateQueryBuilder(new Template(query.toString(), ScriptType.INLINE, "mockscript",
            XContentType.JSON, Collections.emptyMap())).boost(3);
        assertEquals(new BoolQueryBuilder().must(query).boost(3), builder.rewrite(createShardContext()));
    }

    @Override
    protected Query rewrite(Query query) throws IOException {
        // TemplateQueryBuilder adds some optimization if the template and query builder have boosts / query names that wraps
        // the actual QueryBuilder that comes from the template into a BooleanQueryBuilder to give it an outer boost / name
        // this causes some queries to be not exactly equal but equivalent such that we need to rewrite them before comparing.
        if (query != null) {
            MemoryIndex idx = new MemoryIndex();
            return idx.createSearcher().rewrite(query);
        }
        return new MatchAllDocsQuery(); // null == *:*
    }
}
