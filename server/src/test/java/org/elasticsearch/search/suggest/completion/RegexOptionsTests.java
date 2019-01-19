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

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.query.RegexpFlag;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

public class RegexOptionsTests extends ESTestCase {

    private static final int NUMBER_OF_RUNS = 20;

    public static RegexOptions randomRegexOptions() {
        final RegexOptions.Builder builder = RegexOptions.builder();
        maybeSet(builder::setMaxDeterminizedStates, randomIntBetween(1, 1000));
        StringBuilder sb = new StringBuilder();
        for (RegexpFlag regexpFlag : RegexpFlag.values()) {
            if (randomBoolean()) {
                if (sb.length() != 0) {
                    sb.append("|");
                }
                sb.append(regexpFlag.name());
            }
        }
        maybeSet(builder::setFlags, sb.toString());
        return builder.build();
    }

    protected RegexOptions createMutation(RegexOptions original) throws IOException {
        final RegexOptions.Builder builder = RegexOptions.builder();
        builder.setMaxDeterminizedStates(randomValueOtherThan(original.getMaxDeterminizedStates(), () -> randomIntBetween(1, 10)));
        return builder.build();
    }

    /**
     * Test serialization and deserialization
     */
    public void testSerialization() throws IOException {
        for (int i = 0; i < NUMBER_OF_RUNS; i++) {
            RegexOptions testOptions = randomRegexOptions();
            RegexOptions deserializedModel = copyWriteable(testOptions, new NamedWriteableRegistry(Collections.emptyList()),
                    RegexOptions::new);
            assertEquals(testOptions, deserializedModel);
            assertEquals(testOptions.hashCode(), deserializedModel.hashCode());
            assertNotSame(testOptions, deserializedModel);
        }
    }

    public void testIllegalArgument() {
        final RegexOptions.Builder builder = RegexOptions.builder();
        try {
            builder.setMaxDeterminizedStates(-randomIntBetween(1, Integer.MAX_VALUE));
            fail("max determinized state must be positive");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "maxDeterminizedStates must not be negative");
        }
    }
}
