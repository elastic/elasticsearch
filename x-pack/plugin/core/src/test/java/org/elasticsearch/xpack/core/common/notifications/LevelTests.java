/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class LevelTests extends ESTestCase {

    public void testFromString() {
        assertEquals(Level.INFO, Level.fromString("info"));
        assertEquals(Level.INFO, Level.fromString("INFO"));
        assertEquals(Level.WARNING, Level.fromString("warning"));
        assertEquals(Level.WARNING, Level.fromString("WARNING"));
        assertEquals(Level.ERROR, Level.fromString("error"));
        assertEquals(Level.ERROR, Level.fromString("ERROR"));
    }

    public void testToString() {
        assertEquals("info", Level.INFO.toString());
        assertEquals("warning", Level.WARNING.toString());
        assertEquals("error", Level.ERROR.toString());
    }

    public void testValidOrdinals() {
        assertThat(Level.INFO.ordinal(), equalTo(0));
        assertThat(Level.WARNING.ordinal(), equalTo(1));
        assertThat(Level.ERROR.ordinal(), equalTo(2));
    }
}
