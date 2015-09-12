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

package org.elasticsearch.common.math;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class MathUtilsTests extends ESTestCase {

    @Test
    public void mod() {
        final int iters = scaledRandomIntBetween(1000, 10000);
        for (int i = 0; i < iters; ++i) {
            final int v = rarely() ? Integer.MIN_VALUE : rarely() ? Integer.MAX_VALUE : randomInt();
            final int m = rarely() ? Integer.MAX_VALUE : randomIntBetween(1, Integer.MAX_VALUE);
            final int mod = MathUtils.mod(v, m);
            assertTrue(mod >= 0);
            assertTrue(mod < m);
        }
    }

}
