/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ilm;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.CoreMatchers;

import java.util.EnumSet;

public class OperationModeTests extends ESTestCase {

    public void testIsValidChange() {
        assertFalse(OperationMode.RUNNING.isValidChange(OperationMode.RUNNING));
        assertTrue(OperationMode.RUNNING.isValidChange(OperationMode.STOPPING));
        assertFalse(OperationMode.RUNNING.isValidChange(OperationMode.STOPPED));

        assertTrue(OperationMode.STOPPING.isValidChange(OperationMode.RUNNING));
        assertFalse(OperationMode.STOPPING.isValidChange(OperationMode.STOPPING));
        assertTrue(OperationMode.STOPPING.isValidChange(OperationMode.STOPPED));

        assertTrue(OperationMode.STOPPED.isValidChange(OperationMode.RUNNING));
        assertFalse(OperationMode.STOPPED.isValidChange(OperationMode.STOPPING));
        assertFalse(OperationMode.STOPPED.isValidChange(OperationMode.STOPPED));
    }

    public void testFromName() {
        EnumSet.allOf(OperationMode.class).forEach(e -> assertEquals(OperationMode.fromString(e.name()), e));
    }

    public void testFromNameInvalid() {
        String invalidName = randomAlphaOfLength(10);
        Exception e = expectThrows(IllegalArgumentException.class, () -> OperationMode.fromString(invalidName));
        assertThat(e.getMessage(), CoreMatchers.containsString(invalidName + " is not a valid operation_mode"));
    }
}
