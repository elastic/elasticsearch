/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.startsWith;

public class AliasValidatorTests extends ESTestCase {
    public void testValidatesAliasNames() {
        AliasValidator validator = new AliasValidator();
        Exception e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone(".", null));
        assertEquals("Invalid alias name [.]: must not be '.' or '..'", e.getMessage());
        e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone("..", null));
        assertEquals("Invalid alias name [..]: must not be '.' or '..'", e.getMessage());
        e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone("_cat", null));
        assertEquals("Invalid alias name [_cat]: must not start with '_', '-', or '+'", e.getMessage());
        e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone("-cat", null));
        assertEquals("Invalid alias name [-cat]: must not start with '_', '-', or '+'", e.getMessage());
        e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone("+cat", null));
        assertEquals("Invalid alias name [+cat]: must not start with '_', '-', or '+'", e.getMessage());
        e = expectThrows(InvalidAliasNameException.class, () -> validator.validateAliasStandalone("c*t", null));
        assertThat(e.getMessage(), startsWith("Invalid alias name [c*t]: must not contain the following characters "));

        // Doesn't throw an exception because we allow upper case alias names
        validator.validateAliasStandalone("CAT", null);
    }
}
