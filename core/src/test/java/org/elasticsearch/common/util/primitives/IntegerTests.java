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

package org.elasticsearch.common.util.primitives;

import org.elasticsearch.test.ESTestCase;

public class IntegerTests extends ESTestCase {
    public void testTryParse() {
        assertTryParse(0, "0");
        assertTryParse(0, "-0");
        assertTryParse(1, "1");
        assertTryParse(-1, "-1");
        assertTryParse(12345, "12345");
        assertTryParse(-12345, "-12345");
        for (int i = 0; i < 1 << 20; i++) {
            int value = randomInt();
            assertTryParse(value, Integer.toString(value));
        }
        assertTryParse(Integer.MAX_VALUE, Integer.toString(Integer.MAX_VALUE));
        assertTryParse(Integer.MIN_VALUE, Integer.toString(Integer.MIN_VALUE));
        assertNull(Integers.tryParse(null));
        assertNull(Integers.tryParse(""));
        assertNull(Integers.tryParse("-"));
        assertNull(Integers.tryParse("9999999999999999"));
        assertNull(Integers.tryParse(Long.toString(((long) Integer.MAX_VALUE) + 1)));
        assertNull(Integers.tryParse(Long.toString(((long) Integer.MAX_VALUE) * 10)));
        assertNull(Integers.tryParse(Long.toString(((long) Integer.MIN_VALUE) - 1)));
        assertNull(Integers.tryParse(Long.toString(((long) Integer.MIN_VALUE) * 10)));
        assertNull(Integers.tryParse(Long.toString(Long.MAX_VALUE)));
        assertNull(Integers.tryParse(Long.toString(Long.MIN_VALUE)));
    }

    private static void assertTryParse(Integer expected, String value) {
        assertEquals(expected, Integers.tryParse(value));
    }
}