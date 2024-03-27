/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

/**
 * Tests exceptions related to usage of restricted global values with a retriever.
 */
public class RetrieverBuilderErrorTests extends ESTestCase {

    public void testRetrieverExtractionErrors() throws IOException {
        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"query\": {\"match_all\": {}}, \"retriever\":{\"standard\":{}}}"
            )
        ) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> ssb.parseXContent(parser, true, nf -> true));
            assertEquals("cannot specify [retriever] and [query]", iae.getMessage());
        }

        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"knn\":{\"field\": \"test\", \"k\": 2, \"num_candidates\": 5,"
                    + " \"query_vector\": [1, 2, 3]}, \"retriever\":{\"standard\":{}}}"
            )
        ) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> ssb.parseXContent(parser, true, nf -> true));
            assertEquals("cannot specify [retriever] and [knn]", iae.getMessage());
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"search_after\": [1], \"retriever\":{\"standard\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> ssb.parseXContent(parser, true, nf -> true));
            assertEquals("cannot specify [retriever] and [search_after]", iae.getMessage());
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"terminate_after\": 1, \"retriever\":{\"standard\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> ssb.parseXContent(parser, true, nf -> true));
            assertEquals("cannot specify [retriever] and [terminate_after]", iae.getMessage());
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"sort\": [\"field\"], \"retriever\":{\"standard\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> ssb.parseXContent(parser, true, nf -> true));
            assertEquals("cannot specify [retriever] and [sort]", iae.getMessage());
        }

        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"rescore\": {\"query\": {\"rescore_query\": {\"match_all\": {}}}}, \"retriever\":{\"standard\":{}}}"
            )
        ) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> ssb.parseXContent(parser, true, nf -> true));
            assertEquals("cannot specify [retriever] and [rescore]", iae.getMessage());
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"min_score\": 2, \"retriever\":{\"standard\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> ssb.parseXContent(parser, true, nf -> true));
            assertEquals("cannot specify [retriever] and [min_score]", iae.getMessage());
        }

        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "" + "{\"min_score\": 2, \"query\": {\"match_all\": {}}, \"retriever\":{\"standard\":{}}, \"terminate_after\": 1}"
            )
        ) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> ssb.parseXContent(parser, true, nf -> true));
            assertEquals("cannot specify [retriever] and [query, terminate_after, min_score]", iae.getMessage());
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }
}
