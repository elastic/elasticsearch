/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.HighlighterTestCase;

import java.io.IOException;
import java.util.Map;

public class PlainHighlighterTests extends HighlighterTestCase {

    public void testHighlightPhrase() throws Exception {
        Query query = new PhraseQuery.Builder().add(new Term("field", "foo")).add(new Term("field", "bar")).build();
        QueryScorer queryScorer = new CustomQueryScorer(query);
        org.apache.lucene.search.highlight.Highlighter highlighter = new org.apache.lucene.search.highlight.Highlighter(queryScorer);
        String[] frags = highlighter.getBestFragments(new MockAnalyzer(random()), "field", "bar foo bar foo", 10);
        assertArrayEquals(new String[] { "bar <B>foo</B> <B>bar</B> foo" }, frags);
    }

    public void testOrdering() throws Exception {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "description" : { "type" : "text" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "description": [
                "Lorem Ipsum string Generator that helps to create dummy text for all layout needs.",
                "It has roots in a piece of classical Latin literature from 45 BC, making it search string over 2000 years old."
              ]
            }
            """));

        {

            SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.matchQuery("description", "search string"))
                .highlighter(new HighlightBuilder().field("description").highlighterType("plain").order("score").fragmentSize(50));

            Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
            assertHighlights(
                highlights,
                "description",
                " literature from 45 BC, making it <em>search</em> <em>string</em> over 2000 years old.",
                "Lorem Ipsum <em>string</em> Generator that helps to create"
            );

        }

        {

            SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.matchQuery("description", "string generator"))
                .highlighter(new HighlightBuilder().field("description").highlighterType("plain").order("score").fragmentSize(50));

            Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
            assertHighlights(
                highlights,
                "description",
                "Lorem Ipsum <em>string</em> <em>Generator</em> that helps to create",
                " literature from 45 BC, making it search <em>string</em> over 2000 years old."
            );

        }

        {

            SearchSourceBuilder search = new SearchSourceBuilder().query(
                QueryBuilders.matchQuery("description", "lorem layout needs roots years")
            ).highlighter(new HighlightBuilder().field("description").highlighterType("plain").fragmentSize(20));

            Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
            assertHighlights(
                highlights,
                "description",
                "<em>Lorem</em> Ipsum string",
                " text for all <em>layout</em> <em>needs</em>.",
                "It has <em>roots</em> in a",
                " search string over 2000 <em>years</em> old."
            );

        }
    }

    public void testScriptScoreQuery() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "text" : { "type" : "text" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "text" : "some text" }
            """));

        SearchSourceBuilder search = new SearchSourceBuilder().query(
            QueryBuilders.scriptScoreQuery(
                QueryBuilders.boolQuery().should(QueryBuilders.termQuery("text", "some")).must(QueryBuilders.existsQuery("text")),
                new Script("foo")
            )
        ).highlighter(new HighlightBuilder().field("text").highlighterType("plain"));

        assertHighlights(highlight(mapperService, doc, search), "text", "<em>some</em> text");
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T> T compileScript(Script script, ScriptContext<T> context) {
        if (context == ScoreScript.CONTEXT) {
            // Dummy script for testing ScriptScoreQuery highlighting
            return (T) (ScoreScript.Factory) (params, lookup) -> new ScoreScript.LeafFactory() {
                @Override
                public boolean needs_score() {
                    return true;
                }

                @Override
                public boolean needs_termStats() {
                    return false;
                }

                @Override
                public ScoreScript newInstance(DocReader reader) throws IOException {
                    return new ScoreScript(params, lookup, reader) {
                        @Override
                        public double execute(ExplanationHolder explanation) {
                            return 0;
                        }
                    };
                }
            };
        }
        return super.compileScript(script, context);
    }
}
