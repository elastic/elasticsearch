/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.settings.SecureString.CloseableChars;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SecureStringTests extends ESTestCase {

    public void testCloseableCharsDoesNotModifySecureString() {
        final char[] password = randomAsciiOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        try (CloseableChars closeableChars = secureString.getAsCloseableChars()) {
            assertArrayEquals(password, closeableChars.getUnderlyingChars());
            assertThat(closeableChars.getUnderlyingChars(), not(sameInstance(password)));
        }
        assertSecureStringEqualToChars(password, secureString);
    }

    public void testClosingSecureStringDoesNotModifyCloseableChars() {
        final char[] password = randomAsciiOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        CloseableChars closeableChars = secureString.getAsCloseableChars();
        assertArrayEquals(password, closeableChars.getUnderlyingChars());
        assertThat(closeableChars.getUnderlyingChars(), not(sameInstance(password)));
        final char[] passwordCopy = Arrays.copyOf(password, password.length);
        assertArrayEquals(password, passwordCopy);
        secureString.close();
        assertNotEquals(password[0], passwordCopy[0]);
        assertArrayEquals(passwordCopy, closeableChars.getUnderlyingChars());
    }

    public void testClosingChars() {
        final char[] password = randomAsciiOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        CloseableChars closeableChars = secureString.getAsCloseableChars();
        assertArrayEquals(password, closeableChars.getUnderlyingChars());
        assertThat(closeableChars.getUnderlyingChars(), not(sameInstance(password)));
        closeableChars.close();
        if (randomBoolean()) {
            // close another time and no exception is thrown
            closeableChars.close();
        }
        IllegalStateException e = expectThrows(IllegalStateException.class, closeableChars::getUnderlyingChars);
        assertThat(e.getMessage(), containsString("already been closed"));
    }

    public void testGetCloseableCharsAfterSecureStringClosed() {
        final char[] password = randomAsciiOfLengthBetween(1, 32).toCharArray();
        SecureString secureString = new SecureString(password);
        assertSecureStringEqualToChars(password, secureString);
        secureString.close();
        if (randomBoolean()) {
            // close another time and no exception is thrown
            secureString.close();
        }
        IllegalStateException e = expectThrows(IllegalStateException.class, secureString::getAsCloseableChars);
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
