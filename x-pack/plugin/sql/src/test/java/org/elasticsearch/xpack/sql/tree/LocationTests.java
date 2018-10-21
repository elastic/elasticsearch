/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.tree;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;

public class LocationTests extends ESTestCase {
    public static Location randomLocation() {
        return new Location(between(1, Integer.MAX_VALUE), between(1, Integer.MAX_VALUE));
    }

    public static Location mutate(Location location) {
        List<Function<Location, Location>> options = Arrays.asList(
            l -> new Location(
                randomValueOtherThan(l.getLineNumber(), () -> between(1, Integer.MAX_VALUE)),
                l.getColumnNumber() - 1),
            l -> new Location(
                l.getLineNumber(),
                randomValueOtherThan(l.getColumnNumber() - 1, () -> between(1, Integer.MAX_VALUE))));
        return randomFrom(options).apply(location);
    }

    public void testEqualsAndHashCode() {
        checkEqualsAndHashCode(randomLocation(),
            l -> new Location(l.getLineNumber(), l.getColumnNumber() - 1),
            LocationTests::mutate);
    }
}
