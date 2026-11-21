/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class FingerprintIndexTests extends ESTestCase {

    /** Random puts, replacements and removals must always agree with a {@link HashMap}. */
    public void testMatchesHashMapUnderChurn() {
        final FingerprintIndex index = new FingerprintIndex(randomIntBetween(0, 100));
        final Map<Integer, Integer> model = new HashMap<>();
        final int keySpace = randomFrom(50, 500, 5_000);
        for (int step = 0; step < 200_000; step++) {
            final int key = randomIntBetween(0, keySpace) * (randomBoolean() ? 1 : 0x9E3779B9);
            switch (randomIntBetween(0, 2)) {
                case 0 -> {
                    final int value = randomIntBetween(0, 1_000_000);
                    index.put(key, value);
                    model.put(key, value);
                }
                case 1 -> {
                    index.remove(key);
                    model.remove(key);
                }
                case 2 -> {
                    assertThat(index.get(key), equalTo(model.getOrDefault(key, FingerprintIndex.ABSENT)));
                    final int fresh = randomIntBetween(0, 1_000_000);
                    final int previous = index.getOrPut(key, fresh);
                    assertThat(previous, equalTo(model.getOrDefault(key, FingerprintIndex.ABSENT)));
                    model.putIfAbsent(key, fresh);
                }
                default -> throw new AssertionError();
            }
            assertThat(index.size(), equalTo(model.size()));
        }
        for (Map.Entry<Integer, Integer> e : model.entrySet()) {
            assertThat(index.get(e.getKey()), equalTo(e.getValue()));
        }
    }

    /** Hash codes of consecutive integers (the worst case for a weak hash) must still be found after heavy removal. */
    public void testSequentialHashCodesSurviveRemovals() {
        final FingerprintIndex index = new FingerprintIndex(64);
        for (int i = 0; i < 2_000; i++) {
            index.put(i, i);
        }
        for (int i = 0; i < 2_000; i += 2) {
            index.remove(i);
        }
        for (int i = 0; i < 2_000; i++) {
            assertThat(index.get(i), equalTo(i % 2 == 0 ? FingerprintIndex.ABSENT : i));
        }
        assertThat(index.size(), equalTo(1_000));
    }
}
