/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.support;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.support.Validation.Error;
import org.elasticsearch.xpack.security.support.Validation.Users;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.elasticsearch.xpack.security.user.KibanaUser;

import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ValidationTests extends ESTestCase {
    private static final char[] alphabet = {
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z'
    };

    private static final char[] numbers = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

    private static final char[] allowedFirstChars = concat(alphabet, new char[]{'_'});

    private static final char[] allowedSubsequent = concat(alphabet, numbers, new char[]{'_', '@', '-', '$', '.'});

    static {
        Arrays.sort(allowedFirstChars);
        Arrays.sort(allowedSubsequent);
    }

    static char[] concat(char[]... arrays) {
        int length = 0;
        for (char[] array : arrays) {
            length += array.length;
        }
        char[] newArray = new char[length];
        int i = 0;
        for (char[] array : arrays) {
            System.arraycopy(array, 0, newArray, i, array.length);
            i += array.length;
        }
        return newArray;
    }

    public void testUsersValidateUsername() throws Exception {
        int length = randomIntBetween(1, 30);
        String name = new String(generateValidName(length));
        assertThat(Users.validateUsername(name, false, Settings.EMPTY), nullValue());
    }

    public void testReservedUsernames() {
        final String username = randomFrom(ElasticUser.NAME, KibanaUser.NAME);
        final Error error = Users.validateUsername(username, false, Settings.EMPTY);
        assertNotNull(error);
        assertThat(error.toString(), containsString("is reserved"));
    }

    public void testUsersValidateUsernameInvalidLength() throws Exception {
        int length = frequently() ? randomIntBetween(31, 200) : 0; // invalid length
        char[] name = new char[length];
        if (length > 0) {
            name = generateValidName(length);
        }
        assertThat(Users.validateUsername(new String(name), false, Settings.EMPTY), notNullValue());
    }

    public void testUsersValidateUsernameInvalidCharacters() throws Exception {
        int length = randomIntBetween(1, 30); // valid length
        String name = new String(generateInvalidName(length));
        assertThat(Users.validateUsername(name, false, Settings.EMPTY), notNullValue());
    }

    public void testUsersValidatePassword() throws Exception {
        String passwd = randomAsciiOfLength(randomIntBetween(0, 20));
        logger.info("{}[{}]", passwd, passwd.length());
        if (passwd.length() >= 6) {
            assertThat(Users.validatePassword(passwd.toCharArray()), nullValue());
        } else {
            assertThat(Users.validatePassword(passwd.toCharArray()), notNullValue());
        }
    }

    public void testRolesValidateRoleName() throws Exception {
        int length = randomIntBetween(1, 30);
        String name = new String(generateValidName(length));
        assertThat(Validation.Roles.validateRoleName(name), nullValue());
    }

    public void testReservedRoleName() {
        final String rolename = randomFrom(ReservedRolesStore.names());
        final Error error = Validation.Roles.validateRoleName(rolename);
        assertNotNull(error);
        assertThat(error.toString(), containsString("is reserved"));

        final Error allowed = Validation.Roles.validateRoleName(rolename, true);
        assertNull(allowed);
    }

    public void testRolesValidateRoleNameInvalidLength() throws Exception {
        int length = frequently() ? randomIntBetween(31, 200) : 0; // invalid length
        char[] name = new char[length];
        if (length > 0) {
            name = generateValidName(length);
        }
        assertThat(Users.validateUsername(new String(name), false, Settings.EMPTY), notNullValue());
    }

    public void testRolesValidateRoleNameInvalidCharacters() throws Exception {
        int length = randomIntBetween(1, 30); // valid length
        String name = new String(generateInvalidName(length));
        assertThat(Users.validateUsername(name, false, Settings.EMPTY), notNullValue());
    }

    private static char[] generateValidName(int length) {
        char first = allowedFirstChars[randomIntBetween(0, allowedFirstChars.length - 1)];
        char[] subsequent = new char[length - 1];
        for (int i = 0; i < subsequent.length; i++) {
            subsequent[i] = allowedSubsequent[randomIntBetween(0, allowedSubsequent.length - 1)];
        }
        return concat(new char[]{first}, subsequent);
    }

    private static char[] generateInvalidName(int length) {
        if (length == 1 || randomBoolean()) {
            // invalid name due to characters not allowed in the beginning of the name
            char first;
            while (true) {
                first = randomUnicodeOfLength(1).charAt(0);
                if (Arrays.binarySearch(allowedFirstChars, first) < 0) {
                    break;
                }
            }
            char[] subsequent = new char[length - 1];
            for (int i = 0; i < subsequent.length; i++) {
                subsequent[i] = allowedSubsequent[randomIntBetween(0, allowedSubsequent.length - 1)];
            }
            return concat(new char[]{first}, subsequent);
        }

        // invalid name due to charaters not allowed within the name itself
        char first = allowedFirstChars[randomIntBetween(0, allowedFirstChars.length - 1)];
        char[] subsequent = new char[length - 1];
        for (int i = 0; i < subsequent.length; i++) {
            char c;
            while (true) {
                c = randomUnicodeOfLength(1).charAt(0);
                if (Arrays.binarySearch(allowedSubsequent, c) < 0) {
                    break;
                }
            }
            subsequent[i] = c;
        }
        return concat(new char[]{first}, subsequent);
    }
}
