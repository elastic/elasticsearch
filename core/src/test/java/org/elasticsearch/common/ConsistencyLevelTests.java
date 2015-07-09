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

package org.elasticsearch.common;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class ConsistencyLevelTests extends ElasticsearchTestCase {

    @Test
    public void verifyOrdinalOrder() {
        ConsistencyLevel[] order = new ConsistencyLevel[] {
                ConsistencyLevel.DEFAULT,
                ConsistencyLevel.ONE,
                ConsistencyLevel.QUORUM,
                ConsistencyLevel.ALL_MINUS_1,
                ConsistencyLevel.ALL,
        };
        for (int i = 0; i < order.length; i++) {
            assertThat(order[i].ordinal(), equalTo(i));
        }
        assertThat(ConsistencyLevel.values().length, equalTo(order.length));
    }

    @Test
    public void testONE() {
        assertThat(ConsistencyLevel.fromString("one"), equalTo(ConsistencyLevel.ONE));
        assertThat(ConsistencyLevel.ONE.isMet(randomIntBetween(1, 100), 0), equalTo(false));
        assertThat(ConsistencyLevel.ONE.isMet(randomIntBetween(1, 100), 1), equalTo(true));
        assertThat(ConsistencyLevel.ONE.isMet(randomIntBetween(1, 100), randomIntBetween(2, 100)), equalTo(true));
    }

    @Test
    public void testQUORUM() {
        assertThat(ConsistencyLevel.fromString("quorum"), equalTo(ConsistencyLevel.QUORUM));
        assertThat(ConsistencyLevel.QUORUM.isMet(1, 0), equalTo(false));
        assertThat(ConsistencyLevel.QUORUM.isMet(1, 1), equalTo(true));
        assertThat(ConsistencyLevel.QUORUM.isMet(2, 0), equalTo(false));
        assertThat(ConsistencyLevel.QUORUM.isMet(2, 1), equalTo(true));
        assertThat(ConsistencyLevel.QUORUM.isMet(2, 2), equalTo(true));
        assertThat(ConsistencyLevel.QUORUM.isMet(3, 0), equalTo(false));
        assertThat(ConsistencyLevel.QUORUM.isMet(3, 1), equalTo(false));
        assertThat(ConsistencyLevel.QUORUM.isMet(3, 2), equalTo(true));
        assertThat(ConsistencyLevel.QUORUM.isMet(3, 3), equalTo(true));
        assertThat(ConsistencyLevel.QUORUM.isMet(4, 0), equalTo(false));
        assertThat(ConsistencyLevel.QUORUM.isMet(4, 1), equalTo(false));
        assertThat(ConsistencyLevel.QUORUM.isMet(4, 2), equalTo(false));
        assertThat(ConsistencyLevel.QUORUM.isMet(4, 3), equalTo(true));
        assertThat(ConsistencyLevel.QUORUM.isMet(4, 4), equalTo(true));
        assertThat(ConsistencyLevel.QUORUM.isMet(5, 0), equalTo(false));
        assertThat(ConsistencyLevel.QUORUM.isMet(5, 1), equalTo(false));
        assertThat(ConsistencyLevel.QUORUM.isMet(5, 2), equalTo(false));
        assertThat(ConsistencyLevel.QUORUM.isMet(5, 3), equalTo(true));
        assertThat(ConsistencyLevel.QUORUM.isMet(5, 4), equalTo(true));
        assertThat(ConsistencyLevel.QUORUM.isMet(5, 5), equalTo(true));
    }

    @Test
    public void testALL() {
        assertThat(ConsistencyLevel.fromString("all"), equalTo(ConsistencyLevel.ALL));
        assertThat(ConsistencyLevel.fromString("full"), equalTo(ConsistencyLevel.ALL));
        assertThat(ConsistencyLevel.ALL.isMet(1, 0), equalTo(false));
        assertThat(ConsistencyLevel.ALL.isMet(1, 1), equalTo(true));
        assertThat(ConsistencyLevel.ALL.isMet(2, 0), equalTo(false));
        assertThat(ConsistencyLevel.ALL.isMet(2, 1), equalTo(false));
        assertThat(ConsistencyLevel.ALL.isMet(2, 2), equalTo(true));
    }

    @Test
    public void testALL_MINUS_1() {
        assertThat(ConsistencyLevel.fromString("all-1"), equalTo(ConsistencyLevel.ALL_MINUS_1));
        assertThat(ConsistencyLevel.fromString("full-1"), equalTo(ConsistencyLevel.ALL_MINUS_1));
        assertThat(ConsistencyLevel.ALL_MINUS_1.isMet(1, 0), equalTo(false));
        assertThat(ConsistencyLevel.ALL_MINUS_1.isMet(1, 1), equalTo(true));
        assertThat(ConsistencyLevel.ALL_MINUS_1.isMet(2, 0), equalTo(false));
        assertThat(ConsistencyLevel.ALL_MINUS_1.isMet(2, 1), equalTo(true));
        assertThat(ConsistencyLevel.ALL_MINUS_1.isMet(2, 2), equalTo(true));
    }
}
