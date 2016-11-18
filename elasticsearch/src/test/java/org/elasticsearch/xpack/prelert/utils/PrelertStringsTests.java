/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.utils;


import org.elasticsearch.test.ESTestCase;

public class PrelertStringsTests extends ESTestCase {
    public void testDoubleQuoteIfNotAlphaNumeric() {
        assertEquals("foo2", PrelertStrings.doubleQuoteIfNotAlphaNumeric("foo2"));
        assertEquals("\"fo o\"", PrelertStrings.doubleQuoteIfNotAlphaNumeric("fo o"));
        assertEquals("\" \"", PrelertStrings.doubleQuoteIfNotAlphaNumeric(" "));
        assertEquals("\"ba\\\"r\\\"\"", PrelertStrings.doubleQuoteIfNotAlphaNumeric("ba\"r\""));
    }
}
