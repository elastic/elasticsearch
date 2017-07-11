/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.profile;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResultTests;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResultTests;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureFieldName;
import static org.elasticsearch.test.XContentTestUtils.insertRandomFields;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;;

public class SearchProfileShardResultsTests  extends ESTestCase {

    public static SearchProfileShardResults createTestItem() {
        int size = rarely() ? 0 : randomIntBetween(1, 2);
        Map<String, ProfileShardResult> searchProfileResults = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            List<QueryProfileShardResult> queryProfileResults = new ArrayList<>();
            int queryItems = rarely() ? 0 : randomIntBetween(1, 2);
            for (int q = 0; q < queryItems; q++) {
                queryProfileResults.add(QueryProfileShardResultTests.createTestItem());
            }
            AggregationProfileShardResult aggProfileShardResult = AggregationProfileShardResultTests.createTestItem(1);
            searchProfileResults.put(randomAlphaOfLengthBetween(5, 10), new ProfileShardResult(queryProfileResults, aggProfileShardResult));
        }
        return new SearchProfileShardResults(searchProfileResults);
    }

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to ensure we can parse it
     * back to be forward compatible with additions to the xContent
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        SearchProfileShardResults shardResult = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(shardResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            // The ProfileResults "breakdown" section just consists of key/value pairs, we shouldn't add anything random there
            // also we don't want to insert into the root object here, its just the PROFILE_FIELD itself
            Predicate<String> excludeFilter = (s) -> (s.isEmpty() || s.endsWith(ProfileResult.BREAKDOWN.getPreferredName()));
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        SearchProfileShardResults parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            ensureExpectedToken(parser.nextToken(), XContentParser.Token.START_OBJECT, parser::getTokenLocation);
            ensureFieldName(parser, parser.nextToken(), SearchProfileShardResults.PROFILE_FIELD);
            ensureExpectedToken(parser.nextToken(), XContentParser.Token.START_OBJECT, parser::getTokenLocation);
            parsed = SearchProfileShardResults.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);

    }

}
