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

package org.elasticsearch.search.source;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

public class BigDecimalSupportTests extends ElasticsearchIntegrationTest {

    public static final String TEST_INDEX = "test";
    public static final String TEST_TYPE = "type1";
    private final String verySmallValueAsString = "0.0000000000000000000000000000000000000100";
    private final String veryBigValueAsString = "10000000000000000000000000000000000000.01000";
    BigDecimal verySmallValue = new BigDecimal(verySmallValueAsString);
    BigDecimal veryBigValue = new BigDecimal(veryBigValueAsString);
    String verySmallValueKey = "verySmallValue";
    String veryBigValueKey = "veryBigValue";

    Map<String, Object> data = new HashMap<String, Object>(){{
        put(verySmallValueKey, verySmallValue);
        put(veryBigValueKey, veryBigValue);
    }};

    @Test
    public void testWillPreserveBigDecimalValuesInAMapFaithfully() {
        createIndex(TEST_INDEX);
        ensureGreen();


        index(TEST_INDEX, TEST_TYPE, "1", data);
        refresh();

        SearchResponse response = client().prepareSearch(TEST_INDEX).get();
        Map<String, Object> source = response.getHits().getAt(0).getSource();

        assertValueEquals(source, verySmallValueKey, verySmallValueAsString, verySmallValue);
        assertValueEquals(source, veryBigValueKey, veryBigValueAsString, veryBigValue);
    }

    @Test
    public void testWillPreserveBigDecimalValuesFaithfully() {
        createIndex(TEST_INDEX);
        ensureGreen();


        index(TEST_INDEX, TEST_TYPE, "2",
                verySmallValueKey, verySmallValue,
                veryBigValueKey, veryBigValue
        );
        refresh();

        SearchResponse response = client().prepareSearch(TEST_INDEX).get();
        Map<String, Object> source = response.getHits().getAt(0).getSource();

        assertValueEquals(source, verySmallValueKey, verySmallValueAsString, verySmallValue);
        assertValueEquals(source, veryBigValueKey, veryBigValueAsString, veryBigValue);
    }

    private void assertValueEquals(Map<String, Object> source, String valueKey, String valueAsString, BigDecimal value) {
        Object retrievedSmallValue = source.get(valueKey);
        assertThat(retrievedSmallValue, Matchers.instanceOf(BigDecimal.class));
        assertEquals(valueAsString, ((BigDecimal) retrievedSmallValue).toPlainString());
        assertEquals(value, retrievedSmallValue);
    }

    @Test
    public void testWillPreserveBigDecimalValuesInJson() {
        createIndex(TEST_INDEX);
        ensureGreen();

        Map<String, Object> data = new HashMap<String, Object>(){{
            put(verySmallValueKey, verySmallValue);
            put(veryBigValueKey, veryBigValue);
        }};


        index(TEST_INDEX, TEST_TYPE, "3", data);
        refresh();

        SearchResponse response = client().prepareSearch(TEST_INDEX).get();
        assertThat(response.getHits().getAt(0).getSourceAsString(), Matchers.equalTo("{\"veryBigValue\":" + veryBigValueAsString + ",\"verySmallValue\":" + verySmallValueAsString + "}"));
    }
}
