/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.common.Strings.collectionToDelimitedString;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BoundedDelimitedStringCollectorTests extends ESTestCase {

    private interface TestHarness {
        String getResult(Iterable<?> collection, String delimiter, int appendLimit);

        enum Type {
            COLLECTING,
            ITERATING
        }
    }

    private final TestHarness testHarness;

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return Stream.of(TestHarness.Type.values()).map(x -> new Object[] { x })::iterator;
    }

    public BoundedDelimitedStringCollectorTests(@Name("type") TestHarness.Type testHarnessType) {
        testHarness = switch (testHarnessType) {
            case COLLECTING -> (collection, delimiter, appendLimit) -> {
                final var stringBuilder = new StringBuilder();
                final var collector = new Strings.BoundedDelimitedStringCollector(stringBuilder, delimiter, appendLimit);
                collection.forEach(collector::appendItem);
                collector.finish();
                return stringBuilder.toString();
            };
            case ITERATING -> (collection, delimiter, appendLimit) -> {
                final var stringBuilder = new StringBuilder();
                Strings.collectionToDelimitedStringWithLimit(collection, delimiter, appendLimit, stringBuilder);
                return stringBuilder.toString();
            };
        };
    }

    public void testCollectionToDelimitedStringWithLimitZero() {
        final String delimiter = randomFrom("", ",", ", ", "/");

        final int count = between(0, 100);
        final List<String> strings = new ArrayList<>(count);
        while (strings.size() < count) {
            // avoid starting with a sequence of empty appends, it makes the assertions much messier
            final int minLength = strings.isEmpty() && delimiter.isEmpty() ? 1 : 0;
            strings.add(randomAlphaOfLength(between(minLength, 10)));
        }

        final String completelyTruncatedDescription = testHarness.getResult(strings, delimiter, 0);

        if (count == 0) {
            assertThat(completelyTruncatedDescription, equalTo(""));
        } else if (count == 1) {
            assertThat(completelyTruncatedDescription, equalTo(strings.get(0)));
        } else {
            assertThat(
                completelyTruncatedDescription,
                equalTo(strings.get(0) + delimiter + "... (" + count + " in total, " + (count - 1) + " omitted)")
            );
        }
    }

    public void testCollectionToDelimitedStringWithLimitTruncation() {
        final String delimiter = randomFrom("", ",", ", ", "/");

        final int count = between(2, 100);
        final List<String> strings = new ArrayList<>(count);
        while (strings.size() < count) {
            // avoid empty appends, it makes the assertions much messier
            final int minLength = delimiter.isEmpty() ? 1 : 0;
            strings.add(randomAlphaOfLength(between(minLength, 10)));
        }

        final int fullDescriptionLength = collectionToDelimitedString(strings, delimiter).length();
        final int lastItemSize = strings.get(count - 1).length();
        final int truncatedLength = between(0, fullDescriptionLength - lastItemSize - 1);
        final String truncatedDescription = testHarness.getResult(strings, delimiter, truncatedLength);

        assertThat(truncatedDescription, allOf(containsString("... (" + count + " in total,"), endsWith(" omitted)")));

        assertThat(
            truncatedDescription,
            truncatedDescription.length(),
            lessThanOrEqualTo(truncatedLength + ("0123456789" + delimiter + "... (999 in total, 999 omitted)").length())
        );
    }

    public void testCollectionToDelimitedStringWithLimitNoTruncation() {
        final String delimiter = randomFrom("", ",", ", ", "/");

        final int count = between(1, 100);
        final List<String> strings = new ArrayList<>(count);
        while (strings.size() < count) {
            strings.add(randomAlphaOfLength(between(0, 10)));
        }

        final String fullDescription = collectionToDelimitedString(strings, delimiter);
        for (String string : strings) {
            assertThat(fullDescription, containsString(string));
        }

        final int lastItemSize = strings.get(count - 1).length();
        final int minLimit = fullDescription.length() - lastItemSize;
        final int limit = randomFrom(between(minLimit, fullDescription.length()), between(minLimit, Integer.MAX_VALUE), Integer.MAX_VALUE);

        assertThat(testHarness.getResult(strings, delimiter, limit), equalTo(fullDescription));
    }

}
