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

package org.elasticsearch.action.count;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;

public class CountRequestTests extends ESTestCase {

    @Test
    public void testToSearchRequest() {
        CountRequest countRequest;
        if (randomBoolean()) {
            countRequest = new CountRequest(randomStringArray());
        } else {
            countRequest = new CountRequest();
        }
        if (randomBoolean()) {
            countRequest.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            countRequest.types(randomStringArray());
        }
        if (randomBoolean()) {
            countRequest.routing(randomStringArray());
        }
        if (randomBoolean()) {
            countRequest.preference(randomAsciiOfLengthBetween(1, 10));
        }
        if (randomBoolean()) {
            countRequest.source(new QuerySourceBuilder().setQuery(QueryBuilders.termQuery("field", "value")));
        }
        if (randomBoolean()) {
            countRequest.minScore(randomFloat());
        }
        if (randomBoolean()) {
            countRequest.terminateAfter(randomIntBetween(1, 1000));
        }

        SearchRequest searchRequest = countRequest.toSearchRequest();
        assertThat(searchRequest.indices(), equalTo(countRequest.indices()));
        assertThat(searchRequest.indicesOptions(), equalTo(countRequest.indicesOptions()));
        assertThat(searchRequest.types(), equalTo(countRequest.types()));
        assertThat(searchRequest.routing(), equalTo(countRequest.routing()));
        assertThat(searchRequest.preference(), equalTo(countRequest.preference()));

        if (countRequest.source() == null) {
            assertThat(searchRequest.source(), nullValue());
        } else {
            Map<String, Object> sourceMap = XContentHelper.convertToMap(searchRequest.source(), false).v2();
            assertThat(sourceMap.size(), equalTo(1));
            assertThat(sourceMap.get("query"), notNullValue());
        }

        Map<String, Object> extraSourceMap = XContentHelper.convertToMap(searchRequest.extraSource(), false).v2();
        int count = 1;
        assertThat((Integer)extraSourceMap.get("size"), equalTo(0));
        if (countRequest.minScore() == CountRequest.DEFAULT_MIN_SCORE) {
            assertThat(extraSourceMap.get("min_score"), nullValue());
        } else {
            assertThat(((Number)extraSourceMap.get("min_score")).floatValue(), equalTo(countRequest.minScore()));
            count++;
        }
        if (countRequest.terminateAfter() == SearchContext.DEFAULT_TERMINATE_AFTER) {
            assertThat(extraSourceMap.get("terminate_after"), nullValue());
        } else {
            assertThat((Integer)extraSourceMap.get("terminate_after"), equalTo(countRequest.terminateAfter()));
            count++;
        }
        assertThat(extraSourceMap.size(), equalTo(count));
    }

    private static String[] randomStringArray() {
        int count = randomIntBetween(1, 5);
        String[] indices = new String[count];
        for (int i = 0; i < count; i++) {
            indices[i] = randomAsciiOfLengthBetween(1, 10);
        }
        return indices;
    }
}
