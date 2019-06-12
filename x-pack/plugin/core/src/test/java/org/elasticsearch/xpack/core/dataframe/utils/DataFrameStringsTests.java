/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.utils;

import org.elasticsearch.test.ESTestCase;

public class DataFrameStringsTests extends ESTestCase {

    public void testValidId() {
        assertTrue(DataFrameStrings.isValidId("valid-_id"));
    }

    public void testValidId_givenUppercase() {
        assertFalse(DataFrameStrings.isValidId("MiXedCase"));
    }

    public void testValidId_givenStartsWithUnderScore() {
        assertFalse(DataFrameStrings.isValidId("_this_bit_is_ok"));
    }

    public void testKasValidLengthForId_givenTooLong() {
        StringBuilder sb = new StringBuilder();
        for (int i=0; i<DataFrameStrings.ID_LENGTH_LIMIT; i++) {
            sb.append('#');
        }

        assertTrue(DataFrameStrings.hasValidLengthForId(sb.toString()));

        sb.append('#');
        assertFalse(DataFrameStrings.hasValidLengthForId(sb.toString()));
    }
}

