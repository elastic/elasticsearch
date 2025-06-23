/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules.retriever;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Predicates;
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
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class QueryRuleRetrieverBuilderTests extends AbstractXContentTestCase<QueryRuleRetrieverBuilder> {

    public static QueryRuleRetrieverBuilder createRandomQueryRuleRetrieverBuilder() {
        return new QueryRuleRetrieverBuilder(
            randomBoolean()
                ? List.of(randomAlphaOfLengthBetween(5, 10))
                : List.of(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLengthBetween(5, 10)),
            EnterpriseSearchModuleTestUtils.randomMatchCriteria(),
            TestRetrieverBuilder.createRandomTestRetrieverBuilder(),
            randomIntBetween(1, 100)
        );
    }

    @Override
    protected QueryRuleRetrieverBuilder createTestInstance() {
        return createRandomQueryRuleRetrieverBuilder();
    }

    @Override
    protected QueryRuleRetrieverBuilder doParseInstance(XContentParser parser) throws IOException {
        return (QueryRuleRetrieverBuilder) RetrieverBuilder.parseTopLevelRetrieverBuilder(
            parser,
            new RetrieverParserContext(new SearchUsage(), Predicates.never())
        );
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected String[] getShuffleFieldsExceptions() {
        return new String[] {
            QueryRuleRetrieverBuilder.MATCH_CRITERIA_FIELD.getPreferredName(),
            QueryRuleRetrieverBuilder.RULESET_IDS_FIELD.getPreferredName() };
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
                new ParseField(QueryRuleRetrieverBuilder.NAME),
                (p, c) -> QueryRuleRetrieverBuilder.PARSER.apply(p, (RetrieverParserContext) c)
            )
        );
        return new NamedXContentRegistry(entries);
    }

    public void testParserDefaults() throws IOException {
        // Inner retriever content only sent to parser
        String json = """
            {
                "match_criteria": { "foo": "bar" },
                "ruleset_ids": [ "baz" ],
                "retriever": { "standard": { "query": { "query_string": { "query": "i like pugs" } } } }
            }""";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            QueryRuleRetrieverBuilder parsed = QueryRuleRetrieverBuilder.PARSER.parse(
                parser,
                new RetrieverParserContext(new SearchUsage(), nf -> true)
            );
            assertEquals(DEFAULT_RANK_WINDOW_SIZE, parsed.rankWindowSize());
        }
    }

    public void testQueryRuleRetrieverParsing() throws IOException {
        String restContent = """
            {
                "retriever": {
                    "rule": {
                        "retriever": {
                            "test": {
                                "value": "my-test-retriever"
                            }
                        },
                        "ruleset_ids": [
                            "baz"
                        ],
                        "match_criteria": {
                            "key": "value"
                        },
                        "rank_window_size": 100,
                        "_name": "my_rule_retriever"
                    }
                }
            }""";

        SearchUsageHolder searchUsageHolder = new UsageService().getSearchUsageHolder();
        try (XContentParser jsonParser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder source = new SearchSourceBuilder().parseXContent(jsonParser, true, searchUsageHolder, nf -> true);
            assertThat(source.retriever(), instanceOf(QueryRuleRetrieverBuilder.class));
            QueryRuleRetrieverBuilder parsed = (QueryRuleRetrieverBuilder) source.retriever();
            assertThat(parsed.retrieverName(), equalTo("my_rule_retriever"));
            try (XContentParser parseSerialized = createParser(JsonXContent.jsonXContent, Strings.toString(source))) {
                SearchSourceBuilder deserializedSource = new SearchSourceBuilder().parseXContent(
                    parseSerialized,
                    true,
                    searchUsageHolder,
                    nf -> true
                );
                assertThat(deserializedSource.retriever(), instanceOf(QueryRuleRetrieverBuilder.class));
                QueryRuleRetrieverBuilder deserialized = (QueryRuleRetrieverBuilder) source.retriever();
                assertThat(parsed, equalTo(deserialized));
            }
        }
    }

}
