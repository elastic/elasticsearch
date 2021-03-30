/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
