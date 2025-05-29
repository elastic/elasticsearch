/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchbusinessrules.retriever;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
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
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PinnedRetrieverBuilderTests extends AbstractXContentTestCase<PinnedRetrieverBuilder> {

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

    public static PinnedRetrieverBuilder createRandomPinnedRetrieverBuilder() {
        boolean useIds = randomBoolean();
        int numItems = randomIntBetween(1, 5);

        List<String> ids = useIds
            ? IntStream.range(0, numItems).mapToObj(i -> randomAlphaOfLengthBetween(5, 10)).collect(Collectors.toList())
            : null;
        List<SpecifiedDocument> docs = useIds
            ? null
            : IntStream.range(0, numItems)
                .mapToObj(i -> new SpecifiedDocument(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)))
                .collect(Collectors.toList());
        return new PinnedRetrieverBuilder(ids, docs, TestRetrieverBuilder.createRandomTestRetrieverBuilder(), randomIntBetween(1, 100));
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

    public void testValidation() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            new PinnedRetrieverBuilder(
                List.of("id1"),
                List.of(new SpecifiedDocument("id2", "index")),
                new TestRetrieverBuilder("test"),
                DEFAULT_RANK_WINDOW_SIZE
            );
        });
        assertThat(e.getMessage(), equalTo("Both 'ids' and 'docs' cannot be specified at the same time"));

        e = expectThrows(IllegalArgumentException.class, () -> {
            new PinnedRetrieverBuilder(List.of(), List.of(), new TestRetrieverBuilder("test"), DEFAULT_RANK_WINDOW_SIZE);
        });
        assertThat(e.getMessage(), equalTo("Both 'ids' and 'docs' cannot be specified at the same time"));
    }

    public void testValidateSort() {

        PinnedRetrieverBuilder builder = new PinnedRetrieverBuilder(
            List.of("id1"),
            null,
            new TestRetrieverBuilder("test"),
            DEFAULT_RANK_WINDOW_SIZE
        );

        QueryBuilder dummyQuery = new MatchAllQueryBuilder();

        SearchSourceBuilder emptySource = new SearchSourceBuilder();
        emptySource.query(dummyQuery);
        builder.finalizeSourceBuilder(emptySource);
        assertThat(emptySource.sorts(), equalTo(null));

        SearchSourceBuilder scoreSource = new SearchSourceBuilder();
        scoreSource.query(dummyQuery);
        scoreSource.sort("_score");
        builder.finalizeSourceBuilder(scoreSource);
        assertThat(scoreSource.sorts().size(), equalTo(1));

        SearchSourceBuilder customSortSource = new SearchSourceBuilder();
        customSortSource.query(dummyQuery);
        customSortSource.sort("field1");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> builder.finalizeSourceBuilder(customSortSource));
        assertThat(
            e.getMessage(),
            equalTo(
                "["
                    + PinnedRetrieverBuilder.NAME
                    + "] retriever only supports sorting by score, "
                    + "invalid sort criterion: {\n  \"field1\" : {\n    \"order\" : \"asc\"\n  }\n}"
            )
        );

        SearchSourceBuilder multipleSortsSource = new SearchSourceBuilder();
        multipleSortsSource.query(dummyQuery);
        multipleSortsSource.sort("_score");
        multipleSortsSource.sort("field1");
        builder.finalizeSourceBuilder(multipleSortsSource);

        assertThat(multipleSortsSource.sorts().size(), equalTo(2));
        assertThat(multipleSortsSource.sorts().get(0), instanceOf(ScoreSortBuilder.class));
        assertThat(((ScoreSortBuilder) multipleSortsSource.sorts().get(0)).order(), equalTo(SortOrder.DESC));
        assertThat(multipleSortsSource.sorts().get(1), instanceOf(FieldSortBuilder.class));
        assertThat(((FieldSortBuilder) multipleSortsSource.sorts().get(1)).getFieldName(), equalTo("field1"));
        assertThat(((FieldSortBuilder) multipleSortsSource.sorts().get(1)).order(), equalTo(SortOrder.ASC));

        SearchSourceBuilder fieldFirstSource = new SearchSourceBuilder();
        fieldFirstSource.query(dummyQuery);
        fieldFirstSource.sort("field1");
        fieldFirstSource.sort("_score");
        e = expectThrows(IllegalArgumentException.class, () -> builder.finalizeSourceBuilder(fieldFirstSource));
        assertThat(
            e.getMessage(),
            equalTo(
                "["
                    + PinnedRetrieverBuilder.NAME
                    + "] retriever only supports sorting by score, "
                    + "invalid sort criterion: {\n  \"field1\" : {\n    \"order\" : \"asc\"\n  }\n}"
            )
        );
    }
}
