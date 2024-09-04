/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptTermStats;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsArrayWithSize.arrayWithSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ScriptScoreQueryTests extends ESTestCase {

    private Directory dir;
    private IndexWriter w;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private LeafReaderContext leafReaderContext;
    private final SearchLookup lookup = new SearchLookup(null, null, (ctx, doc) -> null);

    @Before
    public void initSearcher() throws IOException {
        dir = newDirectory();
        w = new IndexWriter(dir, newIndexWriterConfig(new StandardAnalyzer()));
        Document d = new Document();
        d.add(new TextField("field", "some text in a field", Field.Store.YES));
        d.add(new TextField("_uid", "1", Field.Store.YES));
        w.addDocument(d);
        w.commit();
        reader = DirectoryReader.open(w);
        searcher = newSearcher(reader);
        leafReaderContext = searcher.getTopReaderContext().leaves().get(0);
    }

    @After
    public void closeAllTheReaders() throws IOException {
        reader.close();
        w.close();
        dir.close();
    }

    public void testExplain() throws IOException {
        Script script = new Script("script using explain");
        ScoreScript.LeafFactory factory = newFactory(script, true, false, explanation -> {
            assertNotNull(explanation);
            explanation.set("this explains the score");
            return 1.0;
        });

        ScriptScoreQuery query = createScriptScoreQuery(Queries.newMatchAllQuery(), script, factory);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        Explanation explanation = weight.explain(leafReaderContext, 0);
        assertNotNull(explanation);
        assertThat(explanation.getDescription(), equalTo("this explains the score"));
        assertThat(explanation.getValue(), equalTo(1.0));
    }

    public void testExplainDefault() throws IOException {
        Script script = new Script("script without setting explanation");
        ScoreScript.LeafFactory factory = newFactory(script, true, false, explanation -> 1.5);

        ScriptScoreQuery query = createScriptScoreQuery(Queries.newMatchAllQuery(), script, factory);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        Explanation explanation = weight.explain(leafReaderContext, 0);
        assertNotNull(explanation);
        String description = explanation.getDescription();
        assertThat(description, containsString("script score function, computed with script:"));
        assertThat(description, containsString("script without setting explanation"));
        assertThat(explanation.getDetails(), arrayWithSize(1));
        assertThat(explanation.getDetails()[0].getDescription(), containsString("_score"));
        assertThat(explanation.getValue(), equalTo(1.5f));
    }

    public void testExplainDefaultNoScore() throws IOException {
        Script script = new Script("script without setting explanation and no score");
        ScoreScript.LeafFactory factory = newFactory(script, false, false, explanation -> 2.0);

        ScriptScoreQuery query = createScriptScoreQuery(Queries.newMatchAllQuery(), script, factory);
        Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);
        Explanation explanation = weight.explain(leafReaderContext, 0);
        assertNotNull(explanation);
        String description = explanation.getDescription();
        assertThat(description, containsString("script score function, computed with script:"));
        assertThat(description, containsString("script without setting explanation and no score"));
        assertThat(explanation.getDetails(), arrayWithSize(0));
        assertThat(explanation.getValue(), equalTo(2.0f));
    }

    public void testScriptScoreErrorOnNegativeScore() {
        Script script = new Script("script that returns a negative score");
        ScoreScript.LeafFactory factory = newFactory(script, false, false, explanation -> -1000.0);
        ScriptScoreQuery query = createScriptScoreQuery(Queries.newMatchAllQuery(), script, factory);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> searcher.search(query, 1));
        assertTrue(e.getMessage().contains("Must be a non-negative score!"));
    }

    public void testScriptTermStatsAvailable() throws IOException {
        Script script = new Script("termStats script without setting explanation");
        ScoreScript scoreScriptMock = mock(ScoreScript.class);
        ScoreScript.LeafFactory factory = newFactory(false, true, (lookup, docReader) -> scoreScriptMock);

        ScriptScoreQuery query = createScriptScoreQuery(
            new BooleanQuery.Builder().add(new TermQuery(new Term("field", "text")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("field", "missing")), BooleanClause.Occur.SHOULD)
                .build(),
            script,
            factory
        );

        query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f).scorer(leafReaderContext);

        ArgumentCaptor<ScriptTermStats> scriptTermStats = ArgumentCaptor.forClass(ScriptTermStats.class);
        verify(scoreScriptMock)._setTermStats(scriptTermStats.capture());
        assertThat(scriptTermStats.getValue().uniqueTermsCount(), equalTo(2));
    }

    public void testScriptTermStatsNotAvailable() throws IOException {
        Script script = new Script("termStats script without setting explanation");
        ScoreScript scoreScriptMock = mock(ScoreScript.class);
        ScoreScript.LeafFactory factory = newFactory(false, false, (lookup, docReader) -> scoreScriptMock);

        ScriptScoreQuery query = createScriptScoreQuery(
            new BooleanQuery.Builder().add(new TermQuery(new Term("field", "text")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term("field", "missing")), BooleanClause.Occur.SHOULD)
                .build(),
            script,
            factory
        );

        query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f).scorer(leafReaderContext);
        verify(scoreScriptMock, never())._setTermStats(any());
    }

    private ScriptScoreQuery createScriptScoreQuery(Query subQuery, Script script, ScoreScript.LeafFactory factory) {
        return new ScriptScoreQuery(subQuery, script, factory, lookup, null, "index", 0, IndexVersion.current());
    }

    private ScoreScript.LeafFactory newFactory(
        Script script,
        boolean needsScore,
        boolean needsTermStats,
        Function<ScoreScript.ExplanationHolder, Double> function
    ) {
        return newFactory(needsScore, needsTermStats, (lookup, docReader) -> new ScoreScript(script.getParams(), lookup, docReader) {
            @Override
            public double execute(ExplanationHolder explanation) {
                return function.apply(explanation);
            }
        });
    }

    private ScoreScript.LeafFactory newFactory(
        boolean needsScore,
        boolean needsTermStats,
        BiFunction<SearchLookup, DocReader, ScoreScript> scopreScriptProvider
    ) {
        SearchLookup lookup = mock(SearchLookup.class);
        LeafSearchLookup leafLookup = mock(LeafSearchLookup.class);
        when(lookup.getLeafSearchLookup(any())).thenReturn(leafLookup);

        return new ScoreScript.LeafFactory() {
            @Override
            public boolean needs_score() {
                return needsScore;
            }

            @Override
            public boolean needs_termStats() {
                return needsTermStats;
            }

            @Override
            public ScoreScript newInstance(DocReader docReader) {
                return scopreScriptProvider.apply(lookup, docReader);
            }
        };
    }
}
