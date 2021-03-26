/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.test.ESTestCase;

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
}
