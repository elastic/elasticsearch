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

public class CustomUnifiedHighlighterTests extends HighlighterTestCase {

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
            .highlighter(new HighlightBuilder().field("field"));

        Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
        assertHighlights(highlights, "field", "this is <em>some</em> text");
    }

    public void testStoredFieldHighlighting() throws IOException {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : { "type" : "text", "store" : true }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "this is some text" }
            """));

        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "some"))
            .highlighter(new HighlightBuilder().field("field"));

        Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
        assertHighlights(highlights, "field", "this is <em>some</em> text");
    }

    public void testStoredCopyToFieldHighlighting() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field_source" : { "type" : "text", "copy_to" : [ "field" ] },
                "field" : { "type" : "text", "store" : true }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field_source" : "this is some text" }
            """));

        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "some"))
            .highlighter(new HighlightBuilder().field("field"));

        Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
        assertHighlights(highlights, "field", "this is <em>some</em> text");
    }

    public void testUnstoredCopyToFieldHighlighting() throws IOException {
        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field_source" : { "type" : "text", "copy_to" : [ "field" ] },
                "field" : { "type" : "text" }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field_source" : "this is some text" }
            """));

        SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "some"))
            .highlighter(new HighlightBuilder().field("field"));

        Map<String, HighlightField> highlights = highlight(mapperService, doc, search);
        assertHighlights(highlights, "field", "this is <em>some</em> text");
    }

}
