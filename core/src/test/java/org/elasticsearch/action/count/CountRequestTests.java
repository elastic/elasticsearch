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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

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
        final boolean querySet = randomBoolean();
        if (querySet) {
            countRequest.query(QueryBuilders.termQuery("field", "value"));
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
        SearchSourceBuilder source = searchRequest.source();
        assertThat(source.size(), equalTo(0));
        if (querySet) {
            assertThat(source.query(), notNullValue());
        } else {
            assertNull(source.query());
        }
        assertThat(source.minScore(), equalTo(countRequest.minScore()));
        assertThat(source.terminateAfter(), equalTo(countRequest.terminateAfter()));
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
