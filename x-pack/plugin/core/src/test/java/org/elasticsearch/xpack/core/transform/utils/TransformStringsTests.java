/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.utils;

import org.elasticsearch.test.ESTestCase;

public class TransformStringsTests extends ESTestCase {

    public void testValidId() {
        assertTrue(TransformStrings.isValidId("valid-_id"));
    }

    public void testValidId_givenUppercase() {
        assertFalse(TransformStrings.isValidId("MiXedCase"));
    }

    public void testValidId_givenStartsWithUnderScore() {
        assertFalse(TransformStrings.isValidId("_this_bit_is_ok"));
    }

    public void testKasValidLengthForId_givenTooLong() {
        assertTrue(TransformStrings.hasValidLengthForId("#".repeat(TransformStrings.ID_LENGTH_LIMIT)));
        assertFalse(TransformStrings.hasValidLengthForId("#".repeat(TransformStrings.ID_LENGTH_LIMIT + 1)));
    }
}
