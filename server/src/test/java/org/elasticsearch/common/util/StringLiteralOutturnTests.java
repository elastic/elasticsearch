/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.test.ESTestCase;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class StringLiteralOutturnTests extends ESTestCase {
    private static final Logger LOGGER = Logger.getLogger(StringLiteralOutturnTests.class.getName());

    public void testOutturn() {
        // originalKeyContents => internKeyContents => outternKeyContents
        final String originalKeyContents = randomAlphaOfLength(20);
        final String internKeyContents = originalKeyContents.intern();
        final String outturnKeyContents = StringLiteralOutturn.outturn(internKeyContents); // OUTTURN
        // IdentityHashMap<String> would use identityHashCode comparison for get, equals, etc
        assertThat(System.identityHashCode(originalKeyContents) == System.identityHashCode(internKeyContents), is(true));
        assertThat(System.identityHashCode(originalKeyContents) != System.identityHashCode(outturnKeyContents), is(true));

        // SimpleKey wrappers for the Strings
        final Setting.SimpleKey originalKey = new Setting.SimpleKey(originalKeyContents);
        final Setting.SimpleKey internKey = new Setting.SimpleKey(internKeyContents);
        final Setting.SimpleKey outturnKey = new Setting.SimpleKey(outturnKeyContents);
        // IdentityHashMap<Setting,SimpleKey> would use identityHashCode comparison for get, equals, etc
        assertThat(System.identityHashCode(originalKey) != System.identityHashCode(internKey), is(true));
        assertThat(System.identityHashCode(originalKey) != System.identityHashCode(outturnKey), is(true));

        // Maps to contain the SimpleKeys
        final Map<Setting.SimpleKey, Object> originalHashMap = new HashMap<>();
        originalHashMap.put(originalKey, "Expected value");
        final Map<Setting.SimpleKey, Object> internIdentityHashMap = new IdentityHashMap<>();
        internIdentityHashMap.put(internKey, "Expected value");
        final Map<Setting.SimpleKey, Object> outturnMap = StringLiteralOutturn.MapSimpleKey.outturn(internIdentityHashMap); // OUTTURN

        // All keys work in originalHashMap
        assertThat(originalHashMap.containsKey(originalKey), is(true));
        assertThat(originalHashMap.get(originalKey), is(equalTo("Expected value")));
        assertThat(originalHashMap.containsKey(internKey), is(true));
        assertThat(originalHashMap.get(internKey), is(equalTo("Expected value")));
        assertThat(originalHashMap.containsKey(outturnKey), is(true));
        assertThat(originalHashMap.get(outturnKey), is(equalTo("Expected value")));

        // Problem: Only internKey works in internIdentityHashMap
        assertThat(internIdentityHashMap.containsKey(originalKey), is(false));
        assertThat(internIdentityHashMap.get(originalKey), is(nullValue()));
        assertThat(internIdentityHashMap.containsKey(internKey), is(true));
        assertThat(internIdentityHashMap.get(internKey), is(equalTo("Expected value")));
        assertThat(internIdentityHashMap.containsKey(outturnKey), is(false));
        assertThat(internIdentityHashMap.get(outturnKey), is(nullValue()));

        // Fix: All keys work in outturnMap, outturn() fixed the map so originalKey works again
        assertThat(outturnMap.containsKey(originalKey), is(true)); // originalKey works again
        assertThat(outturnMap.get(originalKey), is(equalTo("Expected value"))); // originalKey works again
        assertThat(outturnMap.containsKey(internKey), is(true));
        assertThat(outturnMap.get(internKey), is(equalTo("Expected value")));
        assertThat(outturnMap.containsKey(outturnKey), is(true));
        assertThat(outturnMap.get(outturnKey), is(equalTo("Expected value")));

        // Extra: originalKey and outturnKey should return same values by reference in originalHashMap and outturnMap
        assertThat(originalHashMap.get(originalKey) == originalHashMap.get(outturnKey), is(true)); // by reference
        assertThat(outturnMap.get(originalKey) == outturnMap.get(outturnKey), is(true)); // by reference
    }

    public void testIntermediateCharsets() {
        // This order is roughly from the fastest speed to the slowest speed
        final List<Charset> intermediateCharsets = Arrays.asList(
            StandardCharsets.UTF_8,
            StandardCharsets.UTF_16,
            StandardCharsets.UTF_16BE,
            StandardCharsets.UTF_16LE,
            Charset.forName("UTF_32"),
            Charset.forName("UTF_32BE"),
            Charset.forName("UTF_32LE"),
            Charset.forName("UTF_32"),
            Charset.forName("UTF_32BE_BOM"),
            Charset.forName("UTF_32LE_BOM")
        );
        if (randomBoolean()) {
            Collections.reverse(intermediateCharsets);
        }
        final String beforeIntern = randomAlphaOfLength(20);
        final String intern = beforeIntern.intern();
        final int warmupIterations = 1;
        final int measuredIterations = 10_000;
        final StringBuilder sb = new StringBuilder("Iterations: ").append(measuredIterations).append('\n');
        int iteration;
        long nanoTime;
        for (final Charset charset : intermediateCharsets) {
            for (iteration = 0; iteration < warmupIterations; iteration++) {
                final String outturn = StringLiteralOutturn.outturn(intern, charset); // warmup iteration
                assertThat(outturn, is(equalTo(intern)));
                assertThat(outturn, is(equalTo(beforeIntern)));
            }
            nanoTime = System.nanoTime();
            for (iteration = 0; iteration < measuredIterations; iteration++) {
                StringLiteralOutturn.outturn(intern, charset); // measured iteration
            }
            nanoTime = System.nanoTime() - nanoTime;
            sb.append("Charset: ").append(charset).append(", Time: ").append(nanoTime / 1000000F).append("msec").append('\n');
        }
        LOGGER.info(sb.toString());
    }
}
