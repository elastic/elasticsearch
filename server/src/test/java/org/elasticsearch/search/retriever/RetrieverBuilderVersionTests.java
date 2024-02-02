/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.retriever;

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
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"retriever\":{\"blah\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> ssb.parseXContent(parser, true, nf -> false));
            assertEquals("[standard] retriever is not a supported feature", iae.getMessage());
            ssb.parseXContent(parser, false, nf -> nf == StandardRetrieverBuilder.NODE_FEATURE);
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"retriever\":{\"knn\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> ssb.parseXContent(parser, true, nf -> false));
            assertEquals("[knn] retriever is not a supported feature", iae.getMessage());
            ssb.parseXContent(parser, false, nf -> nf == KnnRetrieverBuilder.NODE_FEATURE);
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedXContents());
    }
}
