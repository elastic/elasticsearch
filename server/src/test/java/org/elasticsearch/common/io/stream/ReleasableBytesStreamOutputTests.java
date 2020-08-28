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

package org.elasticsearch.common.io.stream;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ReleasableBytesStreamOutputTests extends ESTestCase {

    public void testRelease() throws Exception {
        MockBigArrays mockBigArrays =
            new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        try (ReleasableBytesStreamOutput output =
                 getRandomReleasableBytesStreamOutput(mockBigArrays)) {
            output.writeBoolean(randomBoolean());
        }
        MockBigArrays.ensureAllArraysAreReleased();
    }

    private ReleasableBytesStreamOutput getRandomReleasableBytesStreamOutput(
                                                MockBigArrays mockBigArrays) throws IOException {
        ReleasableBytesStreamOutput output = new ReleasableBytesStreamOutput(mockBigArrays);
        if (randomBoolean()) {
            for (int i = 0; i < scaledRandomIntBetween(1, 32); i++) {
                output.write(randomByte());
            }
        }
        return output;
    }
}
