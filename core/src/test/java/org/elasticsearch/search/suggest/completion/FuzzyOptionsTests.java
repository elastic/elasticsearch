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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.Fuzziness;

import java.io.IOException;

public class FuzzyOptionsTests extends WritableTestCase<FuzzyOptions> {

    public static FuzzyOptions randomFuzzyOptions() {
        final FuzzyOptions.Builder builder = FuzzyOptions.builder();
        if (randomBoolean()) {
            maybeSet(builder::setFuzziness, randomFrom(Fuzziness.ZERO, Fuzziness.ONE, Fuzziness.TWO));
        } else {
            maybeSet(builder::setFuzziness, randomFrom(0, 1, 2));
        }
        maybeSet(builder::setFuzzyMinLength, randomIntBetween(0, 10));
        maybeSet(builder::setFuzzyPrefixLength, randomIntBetween(0, 10));
        maybeSet(builder::setMaxDeterminizedStates, randomIntBetween(1, 1000));
        maybeSet(builder::setTranspositions, randomBoolean());
        maybeSet(builder::setUnicodeAware, randomBoolean());
        return builder.build();
    }

    @Override
    protected FuzzyOptions createTestModel() {
        return randomFuzzyOptions();
    }

    @Override
    protected FuzzyOptions createMutation(FuzzyOptions original) throws IOException {
        final FuzzyOptions.Builder builder = FuzzyOptions.builder();
        builder.setFuzziness(original.getEditDistance())
            .setFuzzyPrefixLength(original.getFuzzyPrefixLength())
            .setFuzzyMinLength(original.getFuzzyMinLength())
            .setMaxDeterminizedStates(original.getMaxDeterminizedStates())
            .setTranspositions(original.isTranspositions())
            .setUnicodeAware(original.isUnicodeAware());
        switch (randomIntBetween(0, 5)) {
            case 0:
                builder.setFuzziness(randomValueOtherThan(original.getEditDistance(), () -> randomFrom(0, 1, 2)));
                break;
            case 1:
                builder.setFuzzyPrefixLength(randomValueOtherThan(original.getFuzzyPrefixLength(), () ->
                    randomIntBetween(1, 3)));
                break;
            case 2:
                builder.setFuzzyMinLength(randomValueOtherThan(original.getFuzzyMinLength(), () ->
                    randomIntBetween(1, 3)));
                break;
            case 3:
                builder.setMaxDeterminizedStates(randomValueOtherThan(original.getMaxDeterminizedStates(), () ->
                    randomIntBetween(1, 10)));
                break;
            case 4:
                builder.setTranspositions(!original.isTranspositions());
                break;
            case 5:
                builder.setUnicodeAware(!original.isUnicodeAware());
                break;
        }
        return builder.build();
    }

    @Override
    protected FuzzyOptions readFrom(StreamInput in) throws IOException {
        return new FuzzyOptions(in);
    }

    public void testIllegalArguments() {
        final FuzzyOptions.Builder builder = FuzzyOptions.builder();
        try {
            builder.setFuzziness(-randomIntBetween(1, Integer.MAX_VALUE));
            fail("fuzziness must be > 0");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "fuzziness must be between 0 and 2");
        }
        try {
            builder.setFuzziness(randomIntBetween(3, Integer.MAX_VALUE));
            fail("fuzziness must be < 2");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "fuzziness must be between 0 and 2");
        }
        try {
            builder.setFuzziness(null);
            fail("fuzziness must not be null");
        } catch (NullPointerException e) {
            assertEquals(e.getMessage(), "fuzziness must not be null");
        }

        try {
            builder.setFuzzyMinLength(-randomIntBetween(1, Integer.MAX_VALUE));
            fail("fuzzyMinLength must be >= 0");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "fuzzyMinLength must not be negative");
        }

        try {
            builder.setFuzzyPrefixLength(-randomIntBetween(1, Integer.MAX_VALUE));
            fail("fuzzyPrefixLength must be >= 0");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "fuzzyPrefixLength must not be negative");
        }

        try {
            builder.setMaxDeterminizedStates(-randomIntBetween(1, Integer.MAX_VALUE));
            fail("max determinized state must be >= 0");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "maxDeterminizedStates must not be negative");
        }
    }
}
