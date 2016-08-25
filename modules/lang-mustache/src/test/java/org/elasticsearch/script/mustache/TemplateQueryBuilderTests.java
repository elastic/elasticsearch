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

package org.elasticsearch.script.mustache;

import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TemplateQueryBuilderTests extends AbstractQueryTestCase<TemplateQueryBuilder> {

    /**
     * The query type all template tests will be based on.
     */
    private QueryBuilder templateBase;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(MustachePlugin.class, CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("{ \"match_all\" : {}}",
                    s -> new BytesArray("{ \"match_all\" : {}}"));

            scripts.put("{ \"match_all\" : {\"_name\" : \"foobar\"}}",
                    s -> new BytesArray("{ \"match_all\" : {\"_name\" : \"foobar\"}}"));

            scripts.put("{\n" +
                    "  \"term\" : {\n" +
                    "    \"foo\" : {\n" +
                    "      \"value\" : \"bar\",\n" +
                    "      \"boost\" : 2.0\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", s -> new BytesArray("{\n" +
                    "  \"term\" : {\n" +
                    "    \"foo\" : {\n" +
                    "      \"value\" : \"bar\",\n" +
                    "      \"boost\" : 2.0\n" +
                    "    }\n" +
                    "  }\n" +
                    "}"));
            return scripts;
        }
    }

    @Before
    public void before() {
        templateBase = new MatchQueryBuilder("field", "some values");
    }

    @Override
    protected boolean supportsBoostAndQueryName() {
        return false;
    }

    @Override
    protected TemplateQueryBuilder doCreateTestQueryBuilder() {
        return new TemplateQueryBuilder(new Script(templateBase.toString(), ScriptType.INLINE, "mustache", null, null));
    }

    @Override
    protected void doAssertLuceneQuery(TemplateQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertEquals(rewrite(QueryBuilder.rewriteQuery(templateBase, context).toQuery(context)), rewrite(query));
    }

    public void testIllegalArgument() {
        expectThrows(IllegalArgumentException.class, () -> new TemplateQueryBuilder((Script) null));
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
        } catch (ElasticsearchParseException e) {
            assertTrue(e.getMessage().contains("bogusField"));
        }
    }

    public void testJSONGeneration() throws IOException {
        Map<String, Object> vars = new HashMap<>();
        vars.put("template", "filled");
        TemplateQueryBuilder builder = new TemplateQueryBuilder("I am a $template string", ScriptType.INLINE, vars);
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
        String query = "{\"template\": {\"inline\": \"{\\\"match_{{template}}\\\": {}}\\\"\",\"params\" : {\"template\" : \"all\"}}}";
        Map<String, Object> params = new HashMap<>();
        params.put("template", "all");
        QueryBuilder expectedBuilder = new TemplateQueryBuilder(expectedTemplateString, ScriptType.INLINE, params);
        assertParsedQuery(query, expectedBuilder);
    }

    public void testRawTemplate() throws IOException {
        String expectedTemplateString = "{\"match_{{template}}\":{}}";
        String query = "{\"template\": {\"inline\": {\"match_{{template}}\": {}},\"params\" : {\"template\" : \"all\"}}}";
        Map<String, Object> params = new HashMap<>();
        params.put("template", "all");
        QueryBuilder expectedBuilder = new TemplateQueryBuilder(expectedTemplateString, ScriptType.INLINE, params, XContentType.JSON);
        assertParsedQuery(query, expectedBuilder);
    }

    @Override
    public void testMustRewrite() throws IOException {
        String query = "{ \"match_all\" : {}}";
        QueryBuilder builder = new TemplateQueryBuilder(new Script(query, ScriptType.INLINE, "mockscript",
        Collections.emptyMap(), XContentType.JSON));
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
        QueryBuilder builder = new TemplateQueryBuilder(new Script(query, ScriptType.INLINE, "mockscript",
             Collections.emptyMap(), XContentType.JSON));
        assertEquals(new MatchAllQueryBuilder().queryName("foobar"), builder.rewrite(createShardContext()));

        builder = new TemplateQueryBuilder(new Script(query, ScriptType.INLINE, "mockscript",
            Collections.emptyMap(), XContentType.JSON)).queryName("outer");
        assertEquals(new BoolQueryBuilder().must(new MatchAllQueryBuilder().queryName("foobar")).queryName("outer"),
            builder.rewrite(createShardContext()));
    }

    public void testRewriteWithInnerBoost() throws IOException {
        final TermQueryBuilder query = new TermQueryBuilder("foo", "bar").boost(2);
        QueryBuilder builder = new TemplateQueryBuilder(new Script(query.toString(), ScriptType.INLINE, "mockscript",
            Collections.emptyMap(), XContentType.JSON));
        assertEquals(query, builder.rewrite(createShardContext()));

        builder = new TemplateQueryBuilder(new Script(query.toString(), ScriptType.INLINE, "mockscript",
            Collections.emptyMap(), XContentType.JSON)).boost(3);
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
