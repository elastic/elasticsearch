/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class FuzzyOptionsTests extends ESTestCase {

    private static final int NUMBER_OF_RUNS = 20;

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

    protected FuzzyOptions createMutation(FuzzyOptions original) throws IOException {
        final FuzzyOptions.Builder builder = FuzzyOptions.builder();
        builder.setFuzziness(original.getEditDistance()).setFuzzyPrefixLength(original.getFuzzyPrefixLength())
                .setFuzzyMinLength(original.getFuzzyMinLength()).setMaxDeterminizedStates(original.getMaxDeterminizedStates())
                .setTranspositions(original.isTranspositions()).setUnicodeAware(original.isUnicodeAware());
        List<Runnable> mutators = new ArrayList<>();
        mutators.add(() -> builder.setFuzziness(randomValueOtherThan(original.getEditDistance(), () -> randomFrom(0, 1, 2))));

        mutators.add(
                () -> builder.setFuzzyPrefixLength(randomValueOtherThan(original.getFuzzyPrefixLength(), () -> randomIntBetween(1, 3))));
        mutators.add(() -> builder.setFuzzyMinLength(randomValueOtherThan(original.getFuzzyMinLength(), () -> randomIntBetween(1, 3))));
        mutators.add(() -> builder
                .setMaxDeterminizedStates(randomValueOtherThan(original.getMaxDeterminizedStates(), () -> randomIntBetween(1, 10))));
        mutators.add(() -> builder.setTranspositions(original.isTranspositions() == false));
        mutators.add(() -> builder.setUnicodeAware(original.isUnicodeAware() == false));
        randomFrom(mutators).run();
        return builder.build();
    }

    /**
     * Test serialization and deserialization
     */
    public void testSerialization() throws IOException {
        for (int i = 0; i < NUMBER_OF_RUNS; i++) {
            FuzzyOptions testModel = randomFuzzyOptions();
            FuzzyOptions deserializedModel = copyWriteable(testModel, new NamedWriteableRegistry(Collections.emptyList()),
                    FuzzyOptions::new);
            assertEquals(testModel, deserializedModel);
            assertEquals(testModel.hashCode(), deserializedModel.hashCode());
            assertNotSame(testModel, deserializedModel);
        }
    }

    public void testEqualsAndHashCode() throws IOException {
        for (int i = 0; i < NUMBER_OF_RUNS; i++) {
            checkEqualsAndHashCode(randomFuzzyOptions(),
                    original -> copyWriteable(original, new NamedWriteableRegistry(Collections.emptyList()), FuzzyOptions::new),
                    this::createMutation);
        }
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
