/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.CompoundRetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.search.retriever.TestRetrieverBuilder;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.usage.SearchUsage;
import org.elasticsearch.usage.SearchUsageHolder;
import org.elasticsearch.usage.UsageService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.retriever.CompoundRetrieverBuilder.convertToRetrieverSource;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RRFRetrieverBuilderParsingTests extends AbstractXContentTestCase<RRFRetrieverBuilder> {

    /**
     * Creates a random {@link RRFRetrieverBuilder}. The created instance
     * is not guaranteed to pass {@link SearchRequest} validation. This is purely
     * for x-content testing.
     */
    public static RRFRetrieverBuilder createRandomRRFRetrieverBuilder() {
        int rankWindowSize = RRFRankBuilder.DEFAULT_RANK_WINDOW_SIZE;
        if (randomBoolean()) {
            rankWindowSize = randomIntBetween(1, 10000);
        }
        int rankConstant = RRFRetrieverBuilder.DEFAULT_RANK_CONSTANT;
        if (randomBoolean()) {
            rankConstant = randomIntBetween(1, 1000000);
        }

        List<String> fields = null;
        String query = null;
        if (randomBoolean()) {
            fields = randomList(1, 10, () -> randomAlphaOfLengthBetween(1, 10));
            query = randomAlphaOfLengthBetween(1, 10);
        }

        int retrieverCount = randomIntBetween(2, 50);
        List<CompoundRetrieverBuilder.RetrieverSource> innerRetrievers = new ArrayList<>(retrieverCount);
        while (retrieverCount > 0) {
            innerRetrievers.add(convertToRetrieverSource(TestRetrieverBuilder.createRandomTestRetrieverBuilder()));
            --retrieverCount;
        }

        return new RRFRetrieverBuilder(innerRetrievers, fields, query, rankWindowSize, rankConstant);
    }

    @Override
    protected RRFRetrieverBuilder createTestInstance() {
        return createRandomRRFRetrieverBuilder();
    }

    @Override
    protected RRFRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return (RRFRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
            parser,
            new RetrieverParserContext(new SearchUsage(), nf -> true)
        );
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

    public void testRRFRetrieverParsing() throws IOException {
        String restContent = """
            {
              "retriever": {
                "rrf": {
                  "retrievers": [
                    {
                      "test": {
                        "value": "foo"
                      }
                    },
                    {
                      "test": {
                        "value": "bar"
                      }
                    }
                  ],
                  "fields": ["field1", "field2"],
                  "query": "baz",
                  "rank_window_size": 100,
                  "rank_constant": 10,
                  "min_score": 20.0,
                  "_name": "foo_rrf"
                }
              }
            }
            """;
        SearchUsageHolder searchUsageHolder = new UsageService().getSearchUsageHolder();
        try (XContentParser jsonParser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder source = new SearchSourceBuilder().parseXContent(jsonParser, true, searchUsageHolder, nf -> true);
            assertThat(source.retriever(), instanceOf(RRFRetrieverBuilder.class));
            RRFRetrieverBuilder parsed = (RRFRetrieverBuilder) source.retriever();
            assertThat(parsed.minScore(), equalTo(20f));
            assertThat(parsed.retrieverName(), equalTo("foo_rrf"));
            try (XContentParser parseSerialized = createParser(JsonXContent.jsonXContent, Strings.toString(source))) {
                SearchSourceBuilder deserializedSource = new SearchSourceBuilder().parseXContent(
                    parseSerialized,
                    true,
                    searchUsageHolder,
                    nf -> true
                );
                assertThat(deserializedSource.retriever(), instanceOf(RRFRetrieverBuilder.class));
                RRFRetrieverBuilder deserialized = (RRFRetrieverBuilder) source.retriever();
                assertThat(parsed, equalTo(deserialized));
            }
        }
    }
}
