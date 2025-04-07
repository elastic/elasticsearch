/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules.retriever;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.builder.SearchSourceBuilder;
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
import org.elasticsearch.xpack.searchbusinessrules.SpecifiedDocument;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PinnedRetrieverBuilderTests extends AbstractXContentTestCase<PinnedRetrieverBuilder> {

    public static PinnedRetrieverBuilder createRandomPinnedRetrieverBuilder() {
        boolean useIds = randomBoolean();
        boolean useDocs = !useIds || randomBoolean();

        List<String> ids = useIds ? List.of(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)) : new ArrayList<>();
        List<SpecifiedDocument> docs = useDocs
            ? List.of(
                new SpecifiedDocument(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)),
                new SpecifiedDocument(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10))
            )
            : new ArrayList<>();

        return new PinnedRetrieverBuilder(ids, docs, TestRetrieverBuilder.createRandomTestRetrieverBuilder(), randomIntBetween(1, 100));
    }

    @Override
    protected PinnedRetrieverBuilder createTestInstance() {
        return createRandomPinnedRetrieverBuilder();
    }

    @Override
    protected PinnedRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return (PinnedRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
            parser,
            new RetrieverParserContext(new SearchUsage(), nf -> true)
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] { PinnedRetrieverBuilder.IDS_FIELD.getPreferredName(), PinnedRetrieverBuilder.DOCS_FIELD.getPreferredName() };
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new SearchModule(Settings.EMPTY, List.of()).getNamedXContents();
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
                new ParseField(PinnedRetrieverBuilder.NAME),
                (p, c) -> PinnedRetrieverBuilder.PARSER.apply(p, (RetrieverParserContext) c)
            )
        );
        return new NamedXContentRegistry(entries);
    }

    public void testParserDefaults() throws IOException {
        // Inner retriever content only sent to parser
        String json = """
            {
                "ids": [ "id1", "id2" ],
                "retriever": { "standard": { "query": { "query_string": { "query": "i like pugs" } } } }
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            PinnedRetrieverBuilder parsed = PinnedRetrieverBuilder.PARSER.parse(
                parser,
                new RetrieverParserContext(new SearchUsage(), nf -> true)
            );
            assertEquals(DEFAULT_RANK_WINDOW_SIZE, parsed.rankWindowSize());
        }
    }

    public void testPinnedRetrieverParsing() throws IOException {
        String restContent = """
            {
                "retriever": {
                    "pinned": {
                        "retriever": {
                            "test": {
                                "value": "my-test-retriever"
                            }
                        },
                        "ids": [
                            "id1",
                            "id2"
                        ],
                        "docs": [
                            {
                                "_index": "index1",
                                "_id": "doc1"
                            },
                            {
                                "_index": "index2",
                                "_id": "doc2"
                            }
                        ],
                        "rank_window_size": 100,
                        "_name": "my_pinned_retriever"
                    }
                }
            }""";
        SearchUsageHolder searchUsageHolder = new UsageService().getSearchUsageHolder();
        try (XContentParser jsonParser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder source = new SearchSourceBuilder().parseXContent(jsonParser, true, searchUsageHolder, nf -> true);
            assertThat(source.retriever(), instanceOf(PinnedRetrieverBuilder.class));
            PinnedRetrieverBuilder parsed = (PinnedRetrieverBuilder) source.retriever();
            assertThat(parsed.retrieverName(), equalTo("my_pinned_retriever"));
            try (XContentParser parseSerialized = createParser(JsonXContent.jsonXContent, Strings.toString(source))) {
                SearchSourceBuilder deserializedSource = new SearchSourceBuilder().parseXContent(
                    parseSerialized,
                    true,
                    searchUsageHolder,
                    nf -> true
                );
                assertThat(deserializedSource.retriever(), instanceOf(PinnedRetrieverBuilder.class));
                PinnedRetrieverBuilder deserialized = (PinnedRetrieverBuilder) deserializedSource.retriever();
                assertThat(parsed, equalTo(deserialized));
            }
        }
    }
}
