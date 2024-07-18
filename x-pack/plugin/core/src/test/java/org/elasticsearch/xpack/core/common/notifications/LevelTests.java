/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.common.notifications;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class LevelTests extends ESTestCase {

    public void testFromString() {
        assertThat(Level.fromString("info"), equalTo(Level.INFO));
        assertThat(Level.fromString("INFO"), equalTo(Level.INFO));
        assertThat(Level.fromString("warning"), equalTo(Level.WARNING));
        assertThat(Level.fromString("WARNING"), equalTo(Level.WARNING));
        assertThat(Level.fromString("error"), equalTo(Level.ERROR));
        assertThat(Level.fromString("ERROR"), equalTo(Level.ERROR));
    }

    public void testToString() {
        assertThat(Level.INFO.toString(), equalTo("info"));
        assertThat(Level.WARNING.toString(), equalTo("warning"));
        assertThat(Level.ERROR.toString(), equalTo("error"));
    }

    public void testValidOrdinals() {
        assertThat(Level.INFO.ordinal(), equalTo(0));
        assertThat(Level.WARNING.ordinal(), equalTo(1));
        assertThat(Level.ERROR.ordinal(), equalTo(2));
    }

    public void testLog4JLevel() {
        assertThat(Level.INFO.log4jLevel(), equalTo(org.apache.logging.log4j.Level.INFO));
        assertThat(Level.WARNING.log4jLevel(), equalTo(org.apache.logging.log4j.Level.WARN));
        assertThat(Level.ERROR.log4jLevel(), equalTo(org.apache.logging.log4j.Level.ERROR));
    }
}
