/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptQueryBuilderTests extends AbstractQueryTestCase<ScriptQueryBuilder> {
    @Override
    protected ScriptQueryBuilder doCreateTestQueryBuilder() {
        String script = "1";
        Map<String, Object> params = Collections.emptyMap();
        return new ScriptQueryBuilder(new Script(ScriptType.INLINE, MockScriptEngine.NAME, script, params));
    }

    @Override
    protected boolean builderGeneratesCacheableQueries() {
        return false;
    }

    @Override
    protected void doAssertLuceneQuery(ScriptQueryBuilder queryBuilder, Query query, SearchExecutionContext context) throws IOException {
        assertThat(query, instanceOf(ScriptQueryBuilder.ScriptQuery.class));
    }

    public void testIllegalConstructorArg() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> new ScriptQueryBuilder((Script) null));
        assertEquals("script cannot be null", e.getMessage());
    }

    public void testFromJsonVerbose() throws IOException {
        String json =
            "{\n" +
                "  \"script\" : {\n" +
                "    \"script\" : {\n" +
                "      \"source\" : \"5\",\n" +
                "      \"lang\" : \"mockscript\"\n" +
                "    },\n" +
                "    \"boost\" : 1.0,\n" +
                "    \"_name\" : \"PcKdEyPOmR\"\n" +
                "  }\n" +
                "}";

        ScriptQueryBuilder parsed = (ScriptQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "mockscript", parsed.script().getLang());
    }

    public void testFromJson() throws IOException {
        String json =
            "{\n" +
                "  \"script\" : {\n" +
                "    \"script\" : \"5\"," +
                "    \"boost\" : 1.0,\n" +
                "    \"_name\" : \"PcKdEyPOmR\"\n" +
                "  }\n" +
                "}";

        ScriptQueryBuilder parsed = (ScriptQueryBuilder) parseQuery(json);
        assertEquals(json, "5", parsed.script().getIdOrCode());
    }

    public void testArrayOfScriptsException() {
        String json =
            "{\n" +
                "  \"script\" : {\n" +
                "    \"script\" : [ {\n" +
                "      \"source\" : \"5\",\n" +
                "      \"lang\" : \"mockscript\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"source\" : \"6\",\n" +
                "      \"lang\" : \"mockscript\"\n" +
                "    }\n ]" +
                "  }\n" +
                "}";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(e.getMessage(), containsString("does not support an array of scripts"));
    }

    @Override
    protected Map<String, String> getObjectsHoldingArbitraryContent() {
        // script_score.script.params can contain arbitrary parameters. no error is expected when
        // adding additional objects within the params object.
        return Collections.singletonMap(Script.PARAMS_PARSE_FIELD.getPreferredName(), null);
    }

    /**
     * Check that this query is generally not cacheable
     */
    @Override
    public void testCacheability() throws IOException {
        ScriptQueryBuilder queryBuilder = createTestQueryBuilder();
        SearchExecutionContext context = createSearchExecutionContext();
        QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new SearchExecutionContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertFalse("query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    public void testDisallowExpensiveQueries() {
        SearchExecutionContext searchExecutionContext = mock(SearchExecutionContext.class);
        when(searchExecutionContext.allowExpensiveQueries()).thenReturn(false);

        ScriptQueryBuilder queryBuilder = doCreateTestQueryBuilder();
        ElasticsearchException e = expectThrows(ElasticsearchException.class,
                () -> queryBuilder.toQuery(searchExecutionContext));
        assertEquals("[script] queries cannot be executed when 'search.allow_expensive_queries' is set to false.",
                e.getMessage());
    }
}
