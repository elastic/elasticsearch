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

package org.elasticsearch.common.rounding;

import org.elasticsearch.test.ElasticsearchTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;



public class RoundingTests extends ElasticsearchTestCase {

    public void testInterval() {
        final long interval = randomIntBetween(1, 100);
        Rounding.Interval rounding = new Rounding.Interval(interval);
        for (int i = 0; i < 1000; ++i) {
            long l = Math.max(randomLong(), Long.MIN_VALUE + interval);
            final long r = rounding.round(l);
            String message = "round(" + l + ", interval=" + interval + ") = " + r;
            assertEquals(message, 0, r % interval);
            assertThat(message, r, lessThanOrEqualTo(l));
            assertThat(message, r + interval, greaterThan(l));
        }
    }

}
