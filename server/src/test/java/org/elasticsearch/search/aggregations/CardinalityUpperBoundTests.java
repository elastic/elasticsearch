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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class CardinalityUpperBoundTests extends ESTestCase {
    public void testNoneMultiply() {
        assertThat(CardinalityUpperBound.NONE.multiply(randomInt()), equalTo(CardinalityUpperBound.NONE));
    }

    public void testOneMultiply() {
        assertThat(CardinalityUpperBound.ONE.multiply(0), equalTo(CardinalityUpperBound.NONE));
        assertThat(CardinalityUpperBound.ONE.multiply(1), equalTo(CardinalityUpperBound.ONE));
        assertThat(CardinalityUpperBound.ONE.multiply(between(2, Integer.MAX_VALUE)), equalTo(CardinalityUpperBound.MANY));
    }

    public void testManyMultiply() {
        assertThat(CardinalityUpperBound.MANY.multiply(0), equalTo(CardinalityUpperBound.NONE));
        assertThat(CardinalityUpperBound.MANY.multiply(between(1, Integer.MAX_VALUE)), equalTo(CardinalityUpperBound.MANY));
    }
}
