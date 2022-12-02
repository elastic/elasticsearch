/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.HighlighterTestCase;

import java.io.IOException;
import java.util.Map;

public class MatchesHighlighterTests extends HighlighterTestCase {

    public void testSimpleTermHighlighting() throws IOException {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : { "type" : "text" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "this is some text" }
            """));

        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "some"))
            .highlighter(new HighlightBuilder().field("field").highlighterType("matches"));

        Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
        assertHighlights(highlights, "field", "this is <em>some</em> text");
    }

    public void testMultipleFieldHighlighting() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "title" : { "type" : "text" },
                "description" : { "type" : "text" },
                "category" : { "type" : "keyword" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "title" : "A tale of two cities",
              "description" : "It's a story about two cities",
              "category" : [ "fiction", "dickens" ] }
            """));

        SearchSourceBuilder search = new SearchSourceBuilder().query(
            QueryBuilders.queryStringQuery("dickens OR cities").field("title").field("description").field("category"))
            .highlighter(new HighlightBuilder().highlighterType("matches").field("title").field("category"));

        Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
        assertHighlights(highlights, "title", "A tale of two <em>cities</em>");
        assertHighlights(highlights, "category", "<em>dickens</em>");
    }

    public void testScoring() throws Exception {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "description" : { "type" : "text" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "description": [
                "Lorem Ipsum string Generator that helps to create dummy text for all layout needs.",
                "It has roots in a string of classical Latin literature from 45 BC, making it search string over 2000 years old."
              ]
            }
            """));

        HighlightBuilder highlight = new HighlightBuilder()
            .field("description")
            .highlighterType("matches")
            .numOfFragments(2)
            .fragmentSize(50);
        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.matchQuery("description", "search string"))
            .highlighter(highlight);

        Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
        assertHighlights(
            highlights,
            "description",
            "Lorem Ipsum <em>string</em> Generator that helps to create ...",
            "...from 45 BC, making it <em>search</em> <em>string</em> over 2000 year..."
        );
    }

    public void testAnalyzedOffsetLimit() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "description" : { "type" : "text" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            {
              "description": [
                "Lorem Ipsum string Generator that helps to create dummy text for all layout needs.",
                "It has roots in a string of classical Latin literature from 45 BC, making it search string over 2000 years old."
              ]
            }
            """));

        HighlightBuilder highlight = new HighlightBuilder()
            .field("description")
            .highlighterType("matches")
            .maxAnalyzedOffset(70)
            .numOfFragments(2)
            .fragmentSize(50);
        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.matchQuery("description", "search string"))
            .highlighter(highlight);

        Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
        assertHighlights(
            highlights,
            "description",
            "Lorem Ipsum <em>string</em> Generator that helps to create ..."
        );
    }

    // matched_fields - use matches from a set of different fields to highlight this one
    public void testMatchedFields() throws IOException {

        // note that this doesn't actually use a different analyzer for the subfield,
        // given restrictions on analyzers in unit tests
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "description" : {
                  "type" : "text",
                  "fields" : {
                    "stemmed" : { "type" : "text" }
                  }
                }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "description" : "Here is some text" }
            """));

        HighlightBuilder highlight = new HighlightBuilder()
            .field(new HighlightBuilder.Field("description").matchedFields("description", "description.stemmed"))
            .highlighterType("matches");
        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.termQuery("description.stemmed", "some"))
            .highlighter(highlight);

        Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
        assertHighlights(
            highlights,
            "description",
            "Here is <em>some</em> text"
        );

    }
}
