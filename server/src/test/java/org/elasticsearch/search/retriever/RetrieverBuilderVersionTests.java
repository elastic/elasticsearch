/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

/** Tests retrievers validate on their own {@link NodeFeature} */
public class RetrieverBuilderVersionTests extends ESTestCase {

    public void testRetrieverVersions() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"retriever\":{\"standard\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            ParsingException iae = expectThrows(ParsingException.class, () -> ssb.parseXContent(parser, true, nf -> false));
            assertEquals("Unknown key for a START_OBJECT in [retriever].", iae.getMessage());
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"retriever\":{\"standard\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            ParsingException iae = expectThrows(
                ParsingException.class,
                () -> ssb.parseXContent(parser, true, nf -> nf == RetrieverBuilder.RETRIEVERS_SUPPORTED)
            );
            assertEquals("unknown retriever [standard]", iae.getMessage());
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"retriever\":{\"standard\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            ssb.parseXContent(
                parser,
                true,
                nf -> nf == RetrieverBuilder.RETRIEVERS_SUPPORTED || nf == StandardRetrieverBuilder.STANDARD_RETRIEVER_SUPPORTED
            );
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"retriever\":{\"knn\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            ParsingException iae = expectThrows(
                ParsingException.class,
                () -> ssb.parseXContent(parser, true, nf -> nf == RetrieverBuilder.RETRIEVERS_SUPPORTED)
            );
            assertEquals("unknown retriever [knn]", iae.getMessage());
        }

        try (
            XContentParser parser = createParser(
                JsonXContent.jsonXContent,
                "{\"retriever\":{\"knn\":{\"field\": \"test\", \"k\": 2, \"num_candidates\": 5, \"query_vector\": [1, 2, 3]}}}"
            )
        ) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            ssb.parseXContent(
                parser,
                true,
                nf -> nf == RetrieverBuilder.RETRIEVERS_SUPPORTED || nf == KnnRetrieverBuilder.KNN_RETRIEVER_SUPPORTED
            );
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }
}
