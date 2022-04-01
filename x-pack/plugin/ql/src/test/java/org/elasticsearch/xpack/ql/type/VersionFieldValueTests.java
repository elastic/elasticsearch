/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.type;

import org.elasticsearch.test.ESTestCase;

public class VersionFieldValueTests extends ESTestCase {

    public void testInvalidInput() {
        expectThrows(IllegalArgumentException.class, () -> new VersionFieldValue(null));
    }

    public void testValid() {
        assertTrue(new VersionFieldValue("1").isValid());
        assertTrue(new VersionFieldValue("1.0").isValid());
        assertTrue(new VersionFieldValue("1.0.0").isValid());
        assertTrue(new VersionFieldValue("1.0.0.2").isValid());
        assertTrue(
            new VersionFieldValue(
                ""
                    + randomIntBetween(0, Integer.MAX_VALUE)
                    + "."
                    + randomIntBetween(0, Integer.MAX_VALUE)
                    + "."
                    + randomIntBetween(0, Integer.MAX_VALUE)
            ).isValid()
        );
        assertTrue(new VersionFieldValue("1.4.0-rc1").isValid());
        assertTrue(new VersionFieldValue("1.0.0.0-SNAPSHOT").isValid());

    }

    public void testInvalid() {
        assertFalse(new VersionFieldValue("-SNAPSHOT").isValid());
        assertFalse(new VersionFieldValue("1.foo.0-SNAPSHOT.1").isValid());
        assertFalse(new VersionFieldValue("rc1").isValid());
        assertFalse(new VersionFieldValue("bad").isValid());
    }

    public void testCompare() {
        assertEquals(new VersionFieldValue("1.2.4"), new VersionFieldValue("1.2.4"));

        // left < right
        String[][] comparisons = {
            { "1.0.0", "1.1.0" },
            { "1.0.0", "1.0.1" },
            { "1.0.0", "2.0.0" },
            { "1.0.0-SNAPSHOT", "1.0.0" },
            { "1.0.0", "1.0.1-SNAPSHOT" },
            { "1.2.0", "1.11.0" },
            { "1.1.2", "1.1.11" },
            { "1.0.0-alpha1", "1.0.0-alpha2" },
            { "1.0.0-alpha", "1.0.0-beta" },
            { "1.0.0-beta", "1.0.0-rc1" },
            { "1000.0.0", "bad" },
            { "1000.0.0", "SNAPSHOT" },
            { "bar", "foo" } };
        for (int i = 0; i < comparisons.length; i++) {
            String[] item = comparisons[i];
            assertTrue(
                "Wrong version comparison " + item[0] + " < " + item[1],
                new VersionFieldValue(item[0]).compareTo(new VersionFieldValue(item[1])) < 0
            );
            assertTrue(
                "Wrong version comparison " + item[1] + " > " + item[0],
                new VersionFieldValue(item[1]).compareTo(new VersionFieldValue(item[0])) > 0
            );
        }

    }
}
