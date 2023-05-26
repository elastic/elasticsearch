/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;

public class SearchProfileResultsTests extends AbstractXContentSerializingTestCase<SearchProfileResults> {
    public static SearchProfileResults createTestItem() {
        int size = rarely() ? 0 : randomIntBetween(1, 2);
        Map<String, SearchProfileShardResult> shards = Maps.newMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            SearchProfileQueryPhaseResult searchResult = SearchProfileQueryPhaseResultTests.createTestItem();
            ProfileResult fetchResult = randomBoolean() ? null : ProfileResultTests.createTestItem(2);
            shards.put(randomAlphaOfLengthBetween(5, 10), new SearchProfileShardResult(searchResult, fetchResult));
        }
        return new SearchProfileResults(shards);
    }

    public void testParseProfileShardId() {
        record TestPattern(String id, SearchProfileResults.ShardProfileId expected) {}

        TestPattern[] cases = new TestPattern[] {
            new TestPattern(null, null),
            new TestPattern("", null),
            new TestPattern("[][][]", null),
            new TestPattern("chsk8ad", null),
            new TestPattern("[UngEVXTBQL-7w5j_tftGAQ][remote1:blogs]", null),  // shardId is missing
            new TestPattern("[UngEVXTBQL-7w5j_tftGAQ][remote1:blogs][xyz]", null),  // shardId must be integer

            new TestPattern(
                "[UngEVXTBQL-7w5j_tftGAQ][remote1:blogs][0]",
                new SearchProfileResults.ShardProfileId("UngEVXTBQL-7w5j_tftGAQ", "blogs", 0, "remote1")
            ),
            new TestPattern(
                "[2m7SW9oIRrirdrwirM1mwQ][.ds-filebeat-8.3.2-2023.05.04-000420][303]",
                new SearchProfileResults.ShardProfileId("2m7SW9oIRrirdrwirM1mwQ", ".ds-filebeat-8.3.2-2023.05.04-000420", 303, null)
            ),
            new TestPattern("[_na_][:indexName_1][303]", new SearchProfileResults.ShardProfileId("_na_", "indexName_1", 303, "")) };

        for (TestPattern testPattern : cases) {
            assertEquals(testPattern.expected, SearchProfileResults.parseProfileShardId(testPattern.id));
        }
    }

    @Override
    protected SearchProfileResults createTestInstance() {
        return createTestItem();
    }

    @Override
    protected SearchProfileResults mutateInstance(SearchProfileResults instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Reader<SearchProfileResults> instanceReader() {
        return SearchProfileResults::new;
    }

    @Override
    protected SearchProfileResults doParseInstance(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        ensureFieldName(parser, parser.nextToken(), SearchProfileResults.PROFILE_FIELD);
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        SearchProfileResults result = SearchProfileResults.fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return result;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return ProfileResultTests.RANDOM_FIELDS_EXCLUDE_FILTER;
    }
}
