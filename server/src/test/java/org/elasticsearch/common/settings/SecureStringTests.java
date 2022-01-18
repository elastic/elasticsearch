/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SecureStringTests extends ESTestCase {

    public void testCloseableCharsDoesNotModifySecureString() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        try (SecureString copy = secureString.clone()) {
            assertArrayEquals(password, copy.getChars());
            assertThat(copy.getChars(), not(sameInstance(password)));
        }
        assertSecureStringEqualToChars(password, secureString);
    }

    public void testClosingSecureStringDoesNotModifyCloseableChars() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        SecureString copy = secureString.clone();
        assertArrayEquals(password, copy.getChars());
        assertThat(copy.getChars(), not(sameInstance(password)));
        final char[] passwordCopy = Arrays.copyOf(password, password.length);
        assertArrayEquals(password, passwordCopy);
        secureString.close();
        assertNotEquals(password[0], passwordCopy[0]);
        assertArrayEquals(passwordCopy, copy.getChars());
    }

    public void testClosingChars() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        SecureString copy = secureString.clone();
        assertArrayEquals(password, copy.getChars());
        assertThat(copy.getChars(), not(sameInstance(password)));
        copy.close();
        if (randomBoolean()) {
            // close another time and no exception is thrown
            copy.close();
        }
        IllegalStateException e = expectThrows(IllegalStateException.class, copy::getChars);
        assertThat(e.getMessage(), containsString("already been closed"));
    }

    public void testGetCloseableCharsAfterSecureStringClosed() {
        final char[] password = randomAlphaOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        secureString.close();
        if (randomBoolean()) {
            // close another time and no exception is thrown
            secureString.close();
        }
        IllegalStateException e = expectThrows(IllegalStateException.class, secureString::clone);
        assertThat(e.getMessage(), containsString("already been closed"));
    }

    private void assertSecureStringEqualToChars(char[] expected, SecureString secureString) {
        int pos = 0;
        for (int i : secureString.chars().toArray()) {
            if (pos >= expected.length) {
                fail("Index " + i + " greated than or equal to array length " + expected.length);
            } else {
                assertEquals(expected[pos++], (char) i);
            }
        }
    }
}
