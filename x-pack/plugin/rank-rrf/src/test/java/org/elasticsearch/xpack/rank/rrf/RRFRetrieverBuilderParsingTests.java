/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RRFRetrieverBuilderParsingTests extends AbstractXContentTestCase<RRFRetrieverBuilder> {

    /**
     * Creates a random {@link RRFRetrieverBuilder}. The created instance
     * is not guaranteed to pass {@link SearchRequest} validation. This is purely
     * for x-content testing.
     */
    public static RRFRetrieverBuilder createRandomRRFRetrieverBuilder() {
        RRFRetrieverBuilder rrfRetrieverBuilder = new RRFRetrieverBuilder();

        if (randomBoolean()) {
            rrfRetrieverBuilder.windowSize = randomIntBetween(1, 10000);
        }

        if (randomBoolean()) {
            rrfRetrieverBuilder.rankConstant = randomIntBetween(1, 1000000);
        }

        int retrieverCount = randomIntBetween(2, 50);
        rrfRetrieverBuilder.retrieverBuilders = new ArrayList<>(retrieverCount);

        while (retrieverCount > 0) {
            rrfRetrieverBuilder.retrieverBuilders.add(TestRetrieverBuilder.createRandomTestRetrieverBuilder());
            --retrieverCount;
        }

        return rrfRetrieverBuilder;
    }

    @Override
    protected RRFRetrieverBuilder createTestInstance() {
        return createRandomRRFRetrieverBuilder();
    }

    @Override
    protected RRFRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return RRFRetrieverBuilder.PARSER.apply(parser, new RetrieverParserContext(new SearchUsage(), nf -> true));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                TestRetrieverBuilder.TEST_SPEC.getName(),
                (p, c) -> TestRetrieverBuilder.TEST_SPEC.getParser().fromXContent(p, (RetrieverParserContext) c),
                TestRetrieverBuilder.TEST_SPEC.getName().getForRestApiVersion()
            )
        );
        entries.add(
            new NamedXContentRegistry.Entry(
                RetrieverBuilder.class,
                new ParseField(RRFRankPlugin.NAME),
                (p, c) -> RRFRetrieverBuilder.PARSER.apply(p, (RetrieverParserContext) c)
            )
        );
        return new NamedXContentRegistry(entries);
    }
}
