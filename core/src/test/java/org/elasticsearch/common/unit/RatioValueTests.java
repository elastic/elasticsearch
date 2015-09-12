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

package org.elasticsearch.common.unit;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

/**
 * Tests for the {@link RatioValue} class
 */
public class RatioValueTests extends ESTestCase {

    @Test
    public void testParsing() {
        assertThat(RatioValue.parseRatioValue("100%").toString(), is("100.0%"));
        assertThat(RatioValue.parseRatioValue("0%").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("-0%").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("15.1%").toString(), is("15.1%"));
        assertThat(RatioValue.parseRatioValue("0.1%").toString(), is("0.1%"));
        assertThat(RatioValue.parseRatioValue("1.0").toString(), is("100.0%"));
        assertThat(RatioValue.parseRatioValue("0").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("-0").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("0.0").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("-0.0").toString(), is("0.0%"));
        assertThat(RatioValue.parseRatioValue("0.151").toString(), is("15.1%"));
        assertThat(RatioValue.parseRatioValue("0.001").toString(), is("0.1%"));
    }

    @Test
    public void testNegativeCase() {
        testInvalidRatio("100.0001%");
        testInvalidRatio("-0.1%");
        testInvalidRatio("1a0%");
        testInvalidRatio("2");
        testInvalidRatio("-0.01");
        testInvalidRatio("0.1.0");
        testInvalidRatio("five");
        testInvalidRatio("1/2");
    }

    public void testInvalidRatio(String r) {
        try {
            RatioValue.parseRatioValue(r);
            fail("Value: [" + r + "] should be an invalid ratio");
        } catch (ElasticsearchParseException e) {
            // success
        }
    }
}
