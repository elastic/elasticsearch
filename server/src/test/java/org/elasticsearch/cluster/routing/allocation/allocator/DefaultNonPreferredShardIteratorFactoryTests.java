/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.allocation.allocator.DefaultNonPreferredShardIteratorFactory.LazilyExpandingShardIterator;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

public class DefaultNonPreferredShardIteratorFactoryTests extends ESTestCase {

    public void testLazilyExpandingIterator() {
        final List<List<String>> allValues = new ArrayList<>();
        final List<String> flatValues = new ArrayList<>();
        IntStream.range(0, randomIntBetween(0, 30)).forEach(i -> {
            int listSize = randomIntBetween(0, 10);
            final var innerList = IntStream.range(0, listSize).mapToObj(j -> (i + "/" + j)).toList();
            allValues.add(innerList);
            flatValues.addAll(innerList);
        });

        Iterator<String> iterator = new LazilyExpandingShardIterator<>(allValues);

        int nextIndex = 0;
        while (true) {
            if (randomBoolean()) {
                assertEquals(iterator.hasNext(), nextIndex < flatValues.size());
            } else {
                if (nextIndex < flatValues.size()) {
                    assertEquals(iterator.next(), flatValues.get(nextIndex++));
                } else {
                    assertThrows(NoSuchElementException.class, iterator::next);
                }
            }
            if (randomBoolean() && nextIndex == flatValues.size()) {
                break;
            }
        }
    }
}
