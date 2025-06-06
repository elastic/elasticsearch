/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;

import java.net.URL;

import static org.hamcrest.Matchers.equalTo;

public class StringsTests extends ESTestCase {
    public void testURL() {
        URL url = ESTestCase.class.getProtectionDomain().getCodeSource().getLocation();
        String path = url.getPath();
        System.out.println("HEY HEY path is " + path);
    }


    public void testIncorrectPattern() {
        AssertionError assertionError = expectThrows(AssertionError.class, () -> Strings.format("%s %s", 1));
        assertThat(
            assertionError.getMessage(),
            equalTo("Exception thrown when formatting [%s %s]. java.util.MissingFormatArgumentException. Format specifier '%s'")
        );
    }

}
