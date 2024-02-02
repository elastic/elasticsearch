/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;

public class RRFRetrieverBuilderTests extends ESTestCase {

    /** Tests the rrf retriever validates on its own {@link NodeFeature} */
    public void testRetrieverVersions() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"retriever\":{\"rrf\":{}}}")) {
            SearchSourceBuilder ssb = new SearchSourceBuilder();
            ParsingException iae = expectThrows(
                ParsingException.class,
                () -> ssb.parseXContent(parser, true, nf -> nf == RetrieverBuilder.NODE_FEATURE)
            );
            assertEquals("unknown retriever [rrf]", iae.getMessage());
        }
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new SearchModule(Settings.EMPTY, List.of()).getNamedXContents();
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                new ParseField(RRFRankPlugin.NAME),
                (p, c) -> RRFRetrieverBuilder.fromXContent(p, (RetrieverParserContext) c)
            )
        );
        return new NamedXContentRegistry(entries);
    }
}
