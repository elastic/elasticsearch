/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link LicenseState} correctness.
 * <p>
 * If you change the behavior of these tests, then it means that licensing changes across the products!
 */
public class LicenseStateTests extends ESTestCase {
    public void testIsActive() {
        assertThat(LicenseState.DISABLED.isActive(), equalTo(false));

        // all other states are considered active; loop will catch any new state
        for (LicenseState state : LicenseState.values()) {
            if (state != LicenseState.DISABLED) {
                assertThat(state.isActive(), equalTo(true));
            }
        }
    }
}
