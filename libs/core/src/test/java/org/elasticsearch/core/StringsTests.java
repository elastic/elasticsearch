/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class StringsTests extends ESTestCase {

    public void testIncorrectPattern() {
        AssertionError assertionError = expectThrows(AssertionError.class, () -> Strings.format("%s %s", 1));
        assertThat(
            assertionError.getMessage(),
            equalTo("Exception thrown when formatting [%s %s]. java.util.MissingFormatArgumentException. Format specifier '%s'")
        );
    }

}
