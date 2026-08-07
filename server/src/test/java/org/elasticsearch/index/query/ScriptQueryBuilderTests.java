/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.store.Directory;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.script.FilterScript;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
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
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new ScriptQueryBuilder((Script) null));
        assertEquals("script cannot be null", e.getMessage());
    }

    public void testFromJsonVerbose() throws IOException {
        String json = """
            {
              "script" : {
                "script" : {
                  "source" : "5",
                  "lang" : "mockscript"
                },
                "boost" : 1.0,
                "_name" : "PcKdEyPOmR"
              }
            }""";

        ScriptQueryBuilder parsed = (ScriptQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "mockscript", parsed.script().getLang());
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "script" : {
                "script" : "5",    "boost" : 1.0,
                "_name" : "PcKdEyPOmR"
              }
            }""";

        ScriptQueryBuilder parsed = (ScriptQueryBuilder) parseQuery(json);
        assertEquals(json, "5", parsed.script().getIdOrCode());
    }

    public void testArrayOfScriptsException() {
        String json = """
            {
              "script" : {
                "script" : [ {
                  "source" : "5",
                  "lang" : "mockscript"
                },
                {
                  "source" : "6",
                  "lang" : "mockscript"
                }
             ]  }
            }""";

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
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> queryBuilder.toQuery(searchExecutionContext));
        assertEquals("[script] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", e.getMessage());
    }

    public void testCancellationCheckWiredForFilterScript() throws IOException {
        SearchLookup lookup = new SearchLookup(null, null, (ctx, doc) -> null);
        AtomicReference<FilterScript> capturedScript = new AtomicReference<>();
        FilterScript.LeafFactory leafFactory = docReader -> {
            FilterScript script = new FilterScript(Map.of(), null, docReader) {
                @Override
                public boolean execute() {
                    return false;
                }
            };
            capturedScript.set(script);
            return script;
        };
        ScriptQueryBuilder.ScriptQuery query = new ScriptQueryBuilder.ScriptQuery(new Script("test"), leafFactory, lookup);

        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(new Document());
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher contextSearcher = new ContextIndexSearcher(
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true
                );
                query.createWeight(contextSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f).scorerSupplier(reader.leaves().get(0));
                assertNotNull(capturedScript.get()._getCancellationCheck());

                capturedScript.set(null);
                IndexSearcher plainSearcher = newSearcher(reader);
                query.createWeight(plainSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f).scorerSupplier(reader.leaves().get(0));
                assertNull(capturedScript.get()._getCancellationCheck());
            }
        }
    }

    public void testNullScript() {
        String json = """
            {
              "script" : {
                "script" : null
              }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(e.getMessage(), containsString("[script] query does not support token [VALUE_NULL]"));
    }

    public void testReportedCost() throws IOException {
        SearchLookup lookup = new SearchLookup(null, null, (ctx, doc) -> null);
        FilterScript.LeafFactory leafFactory = docReader -> new FilterScript(Map.of(), null, docReader) {
            @Override
            public boolean execute() {
                return false;
            }
        };
        ScriptQueryBuilder.ScriptQuery query = new ScriptQueryBuilder.ScriptQuery(new Script("test"), leafFactory, lookup);

        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(new Document());
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher contextSearcher = new ContextIndexSearcher(
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true
                );
                Scorer scorer = query.createWeight(contextSearcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f)
                    .scorerSupplier(reader.leaves().getFirst())
                    .get(DocIdSetIterator.NO_MORE_DOCS);
                assertThat(scorer.iterator().cost(), greaterThan((long) DocIdSetIterator.NO_MORE_DOCS));
            }
        }
    }
}
