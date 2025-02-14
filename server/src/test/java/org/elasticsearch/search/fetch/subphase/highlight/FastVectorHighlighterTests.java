/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.subphase.highlight;

import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.HighlighterTestCase;

import java.io.IOException;

public class FastVectorHighlighterTests extends HighlighterTestCase {

    public void testHighlightingMultiFields() throws IOException {

        MapperService mapperService = createMapperService("""
            { "_doc" : { "properties" : {
                "field" : {
                    "type" : "text",
                    "fields" : {
                        "stemmed" : {
                            "type" : "text",
                            "term_vector" : "with_positions_offsets"
                        }
                    }
                }
            }}}
            """);

        ParsedDocument doc = mapperService.documentMapper().parse(source("""
            { "field" : "here is some text, which is followed by some more text" }
            """));

        {
            // test SimpleFragmentsBuilder case
            SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.termQuery("field.stemmed", "some"))
                .highlighter(new HighlightBuilder().field("field.stemmed").highlighterType("fvh"));

            assertHighlights(
                highlight(mapperService, doc, search),
                "field.stemmed",
                "here is <em>some</em> text, which is followed by <em>some</em> more text"
            );
        }

        {
            // test ScoreOrderFragmentsBuilder case
            SearchSourceBuilder search = new SearchSourceBuilder().query(QueryBuilders.termQuery("field.stemmed", "some"))
                .highlighter(new HighlightBuilder().field("field.stemmed").highlighterType("fvh").numOfFragments(2).fragmentSize(18));

            assertHighlights(
                highlight(mapperService, doc, search),
                "field.stemmed",
                "here is <em>some</em> text, which",
                "followed by <em>some</em> more text"
            );
        }

    }

}
