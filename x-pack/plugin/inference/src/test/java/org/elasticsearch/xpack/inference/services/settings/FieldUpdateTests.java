/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class FieldUpdateTests extends ESTestCase {

    private static final String CURRENT = "current";
    private static final String CLEARED = "cleared";
    private static final String NEW_VALUE = "new";

    public void testAbsent_KeepsCurrentValue() {
        var update = FieldUpdate.<String>absent();
        assertThat(update.resolve(CURRENT, CLEARED), is(CURRENT));
        assertThat(update.resolve(CURRENT), is(CURRENT));
    }

    public void testCleared_ReturnsClearedValue() {
        var update = FieldUpdate.<String>of(null);
        assertThat(update.resolve(CURRENT, CLEARED), is(CLEARED));
    }

    public void testCleared_WithoutClearedValue_ReturnsNull() {
        var update = FieldUpdate.<String>of(null);
        assertThat(update.resolve(CURRENT), is(nullValue()));
    }

    public void testSet_ReplacesWithSuppliedValue() {
        var update = FieldUpdate.of(NEW_VALUE);
        assertThat(update.resolve(CURRENT, CLEARED), is(NEW_VALUE));
        assertThat(update.resolve(CURRENT), is(NEW_VALUE));
    }
}
