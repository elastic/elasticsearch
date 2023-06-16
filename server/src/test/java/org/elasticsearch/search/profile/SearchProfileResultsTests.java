/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
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

    public void testParseCompositeProfileShardId() {
        String indexUuid = UUID.randomUUID().toString(); // not part of composite ID, so can be anything

        String nodeId1 = NodeEnvironment.generateNodeId(Settings.EMPTY);
        String nodeId2 = NodeEnvironment.generateNodeId(
            Settings.builder()
                .put(randomAlphaOfLengthBetween(5, 18), randomNonNegativeInt())
                .put(randomAlphaOfLengthBetween(5, 18), randomAlphaOfLength(15))
                .build()
        );

        int shardId1 = randomNonNegativeInt();
        int shardId2 = randomNonNegativeInt();
        int shardId3 = randomNonNegativeInt();

        String indexName1 = "x";
        String indexName2 = ".ds-filebeat-8.3.2-2023.05.04-000420";
        String indexName3 = randomIdentifier();

        record TestPattern(SearchShardTarget input, SearchProfileResults.ShardProfileId expected) {}

        TestPattern[] validPatterns = new TestPattern[] {
            new TestPattern(
                new SearchShardTarget(nodeId1, new ShardId(indexName1, indexUuid, shardId1), "remote1"),
                new SearchProfileResults.ShardProfileId(nodeId1, indexName1, shardId1, "remote1")
            ),
            new TestPattern(
                new SearchShardTarget(nodeId2, new ShardId(indexName2, indexUuid, shardId2), null),
                new SearchProfileResults.ShardProfileId(nodeId2, indexName2, shardId2, null)
            ),
            new TestPattern(
                new SearchShardTarget(null, new ShardId(indexName3, indexUuid, shardId3), null),
                new SearchProfileResults.ShardProfileId("_na_", indexName3, shardId3, null)
            ) };
        for (TestPattern testPattern : validPatterns) {
            assertEquals(testPattern.expected, SearchProfileResults.parseCompositeProfileShardId(testPattern.input.toString()));
        }
    }

    public void testParseCompositeProfileShardIdWithInvalidEntries() {
        String[] invalidPatterns = new String[] {
            null,
            "",
            "chsk8ad",
            "[UngEVXTBQL-7w5j_tftGAQ][remote1:blogs]",     // shardId is missing
            "[UngEVXTBQL-7w5j_tftGAQ][remote1:blogs][xyz]" // shardId must be integer
        };
        for (String testPattern : invalidPatterns) {
            expectThrows(AssertionError.class, () -> SearchProfileResults.parseCompositeProfileShardId(testPattern));
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
