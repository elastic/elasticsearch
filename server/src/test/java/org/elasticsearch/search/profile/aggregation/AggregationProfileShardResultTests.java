/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.aggregation;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.ProfileResultTests;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;

public class AggregationProfileShardResultTests extends AbstractXContentSerializingTestCase<AggregationProfileShardResult> {

    public static AggregationProfileShardResult createTestItem(int depth) {
        int size = randomIntBetween(0, 5);
        List<ProfileResult> aggProfileResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            aggProfileResults.add(ProfileResultTests.createTestItem(1));
        }
        return new AggregationProfileShardResult(aggProfileResults);
    }

    @Override
    protected AggregationProfileShardResult createTestInstance() {
        return createTestItem(2);
    }

    @Override
    protected AggregationProfileShardResult mutateInstance(AggregationProfileShardResult instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected AggregationProfileShardResult doParseInstance(XContentParser parser) throws IOException {
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParserUtils.ensureFieldName(parser, parser.nextToken(), AggregationProfileShardResult.AGGREGATIONS);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_ARRAY, parser.nextToken(), parser);
        AggregationProfileShardResult result = AggregationProfileShardResult.fromXContent(parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_ARRAY, parser.currentToken(), parser);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return result;
    }

    @Override
    protected Reader<AggregationProfileShardResult> instanceReader() {
        return AggregationProfileShardResult::new;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return ProfileResultTests.RANDOM_FIELDS_EXCLUDE_FILTER;
    }

    public void testToXContent() throws IOException {
        List<ProfileResult> profileResults = new ArrayList<>();
        Map<String, Long> breakdown = new LinkedHashMap<>();
        breakdown.put("timing1", 2000L);
        breakdown.put("timing2", 4000L);
        Map<String, Object> debug = new LinkedHashMap<>();
        debug.put("stuff", "stuff");
        debug.put("other_stuff", List.of("foo", "bar"));
        ProfileResult profileResult = new ProfileResult("someType", "someDescription", breakdown, debug, 6000L, Collections.emptyList());
        profileResults.add(profileResult);
        AggregationProfileShardResult aggProfileResults = new AggregationProfileShardResult(profileResults);
        BytesReference xContent = toXContent(aggProfileResults, XContentType.JSON, false);
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "aggregations": [
                {
                  "type": "someType",
                  "description": "someDescription",
                  "time_in_nanos": 6000,
                  "breakdown": {
                    "timing1": 2000,
                    "timing2": 4000
                  },
                  "debug": {
                    "stuff": "stuff",
                    "other_stuff": [ "foo", "bar" ]
                  }
                }
              ]
            }"""), xContent.utf8ToString());

        xContent = toXContent(aggProfileResults, XContentType.JSON, true);
        assertEquals(XContentHelper.stripWhitespace("""
            {
              "aggregations": [
                {
                  "type": "someType",
                  "description": "someDescription",
                  "time": "6micros",
                  "time_in_nanos": 6000,
                  "breakdown": {
                    "timing1": 2000,
                    "timing2": 4000
                  },
                  "debug": {
                    "stuff": "stuff",
                    "other_stuff": [ "foo", "bar" ]
                  }
                }
              ]
            }"""), xContent.utf8ToString());
    }

}
