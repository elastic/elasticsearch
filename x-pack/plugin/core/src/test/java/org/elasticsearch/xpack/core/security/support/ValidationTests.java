/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.support.Validation.Error;
import org.elasticsearch.xpack.core.security.support.Validation.Roles;
import org.elasticsearch.xpack.core.security.support.Validation.Users;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ValidationTests extends ESTestCase {

    private static final Character[] ALLOWED_CHARS = Validation.VALID_NAME_CHARS.toArray(
        new Character[Validation.VALID_NAME_CHARS.size()]
    );

    private static final Set<Character> VALID_SERVICE_ACCOUNT_TOKEN_NAME_CHARS = Set.of(
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O',
        'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o',
        'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '-', '_'
    );

    private static final Set<Character> INVALID_SERVICE_ACCOUNT_TOKEN_NAME_CHARS = Set.of(
        '!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '.', '/', ';', '<', '=', '>', '?', '@', '[',
        '\\', ']', '^', '`', '{', '|', '}', '~', ' ', '\t', '\n', '\r');

    public void testUsernameValid() throws Exception {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, Validation.MAX_NAME_LENGTH);
        String name = new String(generateValidName(length));
        assertThat(Users.validateUsername(name, false, Settings.EMPTY), nullValue());
    }

    public void testUsernameReserved() {
        final String username = randomFrom(ElasticUser.NAME, KibanaUser.NAME);
        final Error error = Users.validateUsername(username, false, Settings.EMPTY);
        assertNotNull(error);
        assertThat(error.toString(), containsString("is reserved"));
    }

    public void testUsernameInvalidLength() throws Exception {
        int length = frequently() ? randomIntBetween(Validation.MAX_NAME_LENGTH + 1, 2048) : 0;
        char[] name = new char[length];
        if (length > 0) {
            name = generateValidName(length);
        }
        assertThat(Users.validateUsername(new String(name), false, Settings.EMPTY), notNullValue());
    }

    public void testUsernameInvalidCharacters() throws Exception {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, Validation.MAX_NAME_LENGTH);
        String name = new String(generateNameInvalidCharacters(length));
        assertThat(Users.validateUsername(name, false, Settings.EMPTY), notNullValue());
    }

    public void testUsernameInvalidWhitespace() throws Exception {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, Validation.MAX_NAME_LENGTH);
        String name = new String(generateNameInvalidWhitespace(length));
        assertThat(Users.validateUsername(name, false, Settings.EMPTY), notNullValue());
    }

    public void testUsersValidatePassword() throws Exception {
        SecureString passwd = new SecureString(randomAlphaOfLength(randomIntBetween(0, 20)).toCharArray());
        logger.info("{}[{}]", passwd, passwd.length());
        if (passwd.length() >= 6) {
            assertThat(Users.validatePassword(passwd), nullValue());
        } else {
            assertThat(Users.validatePassword(passwd), notNullValue());
        }
    }

    public void testRoleNameValid() throws Exception {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, Validation.MAX_NAME_LENGTH);
        String name = new String(generateValidName(length));
        assertThat(Roles.validateRoleName(name), nullValue());
    }

    public void testRoleNameReserved() {
        final String rolename = randomFrom(ReservedRolesStore.names());
        final Error error = Roles.validateRoleName(rolename);
        assertNotNull(error);
        assertThat(error.toString(), containsString("is reserved"));

        final Error allowed = Roles.validateRoleName(rolename, true);
        assertNull(allowed);
    }

    public void testRoleNameInvalidLength() throws Exception {
        int length = frequently() ? randomIntBetween(Validation.MAX_NAME_LENGTH + 1, 2048) : 0;
        char[] name = new char[length];
        if (length > 0) {
            name = generateValidName(length);
        }
        assertThat(Roles.validateRoleName(new String(name), false), notNullValue());
    }

    public void testRoleNameInvalidCharacters() throws Exception {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, Validation.MAX_NAME_LENGTH);
        String name = new String(generateNameInvalidCharacters(length));
        assertThat(Roles.validateRoleName(name, false), notNullValue());
    }

    public void testRoleNameInvalidWhitespace() throws Exception {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, Validation.MAX_NAME_LENGTH);
        String name = new String(generateNameInvalidWhitespace(length));
        assertThat(Roles.validateRoleName(name, false), notNullValue());
    }

    public void testIsValidServiceAccountTokenName() {
        final String tokenName1 = ValidationTests.randomTokenName();
        assertThat(Validation.isValidServiceAccountTokenName(tokenName1), is(true));

        final String tokenName2 = "_" + ValidationTests.randomTokenName().substring(1);
        assertThat(Validation.isValidServiceAccountTokenName(tokenName2), is(false));

        assertThat(Validation.isValidServiceAccountTokenName(null), is(false));

        final String tokenName3 = ValidationTests.randomInvalidTokenName();
        assertThat(Validation.isValidServiceAccountTokenName(tokenName3), is(false));
    }

    private static char[] generateValidName(int length) {
        char[] name = new char[length];
        name[0] = chooseValidNonWhitespaceCharacter();
        if (length > 1) {
            for (int i = 1; i < length - 1; i++) {
                name[i] = chooseValidCharacter();
            }
        }
        name[length - 1] = chooseValidNonWhitespaceCharacter();
        return name;
    }

    private static char chooseValidCharacter() {
        return randomFrom(ALLOWED_CHARS);
    }

    private static char chooseValidNonWhitespaceCharacter() {
        char c = chooseValidCharacter();
        while (c == ' ') {
            c = chooseValidCharacter();
        }
        return c;
    }

    private static char[] generateNameInvalidCharacters(int length) {
        char[] name = new char[length];
        for (int i = 0; i < length; i++) {
            char c;
            while (true) {
                c = randomUnicodeOfLength(1).charAt(0);
                final char finalChar = c;
                if (Validation.VALID_NAME_CHARS.contains(finalChar) == false) {
                    break;
                }
            }
            name[i] = c;
        }

        return name;
    }

    private static char[] generateNameInvalidWhitespace(int length) {
        char[] name = generateValidName(length);
        if (randomBoolean()) {
            name[0] = ' ';
        } else {
            name[name.length - 1] = ' ';
        }
        return name;
    }

    public static String randomTokenName() {
        final Character[] chars = randomArray(
            1,
            256,
            Character[]::new,
            () -> randomFrom(VALID_SERVICE_ACCOUNT_TOKEN_NAME_CHARS));
        final String name = Arrays.stream(chars).map(String::valueOf).collect(Collectors.joining());
        return name.startsWith("_") ? randomAlphaOfLength(1) + name.substring(1) : name;
    }

    public static String randomInvalidTokenName() {
        if (randomBoolean()) {
            final String tokenName = randomTokenName();
            final char[] chars = tokenName.toCharArray();
            IntStream.rangeClosed(1, randomIntBetween(1, chars.length))
                .forEach(i -> chars[randomIntBetween(0, chars.length - 1)] = randomFrom(INVALID_SERVICE_ACCOUNT_TOKEN_NAME_CHARS));
            return new String(chars);
        } else {
            return randomFrom("", " ", randomAlphaOfLength(257));
        }
    }
}
