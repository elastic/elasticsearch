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
import org.elasticsearch.xpack.core.security.support.Validation.UserManagedServiceAccounts;
import org.elasticsearch.xpack.core.security.support.Validation.Users;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.junit.BeforeClass;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ValidationTests extends ESTestCase {

    @BeforeClass
    public static void setUpClass() {
        // Initialize the reserved roles store so that static fields are populated.
        // In production code, this is guaranteed by how components are initialized by the Security plugin
        new ReservedRolesStore();
    }

    private static final Character[] ALLOWED_CHARS = Validation.VALID_NAME_CHARS.toArray(new Character[Validation.VALID_NAME_CHARS.size()]);

    private static final Set<Character> VALID_SERVICE_ACCOUNT_TOKEN_NAME_CHARS = Set.of(
        '0',
        '1',
        '2',
        '3',
        '4',
        '5',
        '6',
        '7',
        '8',
        '9',
        'A',
        'B',
        'C',
        'D',
        'E',
        'F',
        'G',
        'H',
        'I',
        'J',
        'K',
        'L',
        'M',
        'N',
        'O',
        'P',
        'Q',
        'R',
        'S',
        'T',
        'U',
        'V',
        'W',
        'X',
        'Y',
        'Z',
        'a',
        'b',
        'c',
        'd',
        'e',
        'f',
        'g',
        'h',
        'i',
        'j',
        'k',
        'l',
        'm',
        'n',
        'o',
        'p',
        'q',
        'r',
        's',
        't',
        'u',
        'v',
        'w',
        'x',
        'y',
        'z',
        '-',
        '_'
    );

    private static final Set<Character> INVALID_SERVICE_ACCOUNT_TOKEN_NAME_CHARS = Set.of(
        '!',
        '"',
        '#',
        '$',
        '%',
        '&',
        '\'',
        '(',
        ')',
        '*',
        '+',
        ',',
        '.',
        '/',
        ';',
        '<',
        '=',
        '>',
        '?',
        '@',
        '[',
        '\\',
        ']',
        '^',
        '`',
        '{',
        '|',
        '}',
        '~',
        ' ',
        '\t',
        '\n',
        '\r'
    );

    private static final int CUSTOM_MAX_NAME_LENGTH = 507;

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
        int length = frequently() ? randomIntBetween(Validation.MAX_NAME_LENGTH + 1, Validation.MAX_NAME_LENGTH * 2) : 0;
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

    public void testRoleNameValid() throws Exception {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, Validation.MAX_NAME_LENGTH);
        String name = new String(generateValidName(length));
        assertThat(Roles.validateRoleName(name, false), nullValue());
    }

    public void testRoleNameReserved() {
        final String rolename = randomFrom(ReservedRolesStore.names());
        final Error error = Roles.validateRoleName(rolename, false, Validation.MAX_NAME_LENGTH);
        assertNotNull(error);
        assertThat(error.toString(), containsString("is reserved"));

        final Error allowed = Roles.validateRoleName(rolename, true);
        assertNull(allowed);
    }

    public void testRoleNameInvalidLength() throws Exception {
        int length = frequently() ? randomIntBetween(Validation.MAX_NAME_LENGTH + 1, Validation.MAX_NAME_LENGTH * 2) : 0;
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

    public void testUsernameValidWithCustomLength() {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, CUSTOM_MAX_NAME_LENGTH);
        String name = new String(generateValidName(length));
        assertThat(Users.validateUsername(name, false, Settings.EMPTY, CUSTOM_MAX_NAME_LENGTH), nullValue());
    }

    public void testUsernameReservedWithCustomLength() {
        final String username = randomFrom(ElasticUser.NAME, KibanaUser.NAME);
        final Error error = Users.validateUsername(username, false, Settings.EMPTY, CUSTOM_MAX_NAME_LENGTH);
        assertNotNull(error);
        assertThat(error.toString(), containsString("is reserved"));
    }

    public void testUsernameInvalidLengthWithCustomLength() {
        int length = frequently() ? randomIntBetween(CUSTOM_MAX_NAME_LENGTH + 1, CUSTOM_MAX_NAME_LENGTH * 2) : 0;
        char[] name = new char[length];
        if (length > 0) {
            name = generateValidName(length);
        }
        assertThat(Users.validateUsername(new String(name), false, Settings.EMPTY, CUSTOM_MAX_NAME_LENGTH), notNullValue());
    }

    public void testUsernameInvalidCharactersWithCustomLength() {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, CUSTOM_MAX_NAME_LENGTH);
        String name = new String(generateNameInvalidCharacters(length));
        assertThat(Users.validateUsername(name, false, Settings.EMPTY, CUSTOM_MAX_NAME_LENGTH), notNullValue());
    }

    public void testUsernameInvalidWhitespaceWithCustomLength() {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, CUSTOM_MAX_NAME_LENGTH);
        String name = new String(generateNameInvalidWhitespace(length));
        assertThat(Users.validateUsername(name, false, Settings.EMPTY, CUSTOM_MAX_NAME_LENGTH), notNullValue());
    }

    public void testUsersValidatePassword() {
        SecureString passwd = new SecureString(randomAlphaOfLength(randomIntBetween(0, 20)).toCharArray());
        logger.info("{}[{}]", passwd, passwd.length());
        if (passwd.length() >= 6) {
            assertThat(Users.validatePassword(passwd), nullValue());
        } else {
            assertThat(Users.validatePassword(passwd), notNullValue());
        }
    }

    public void testRoleNameValidWithCustomLength() {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, CUSTOM_MAX_NAME_LENGTH);
        String name = new String(generateValidName(length));
        assertThat(Roles.validateRoleName(name, false, CUSTOM_MAX_NAME_LENGTH), nullValue());
    }

    public void testRoleNameReservedWithCustomLength() {
        final String rolename = randomFrom(ReservedRolesStore.names());
        final Error error = Roles.validateRoleName(rolename, false, CUSTOM_MAX_NAME_LENGTH);
        assertNotNull(error);
        assertThat(error.toString(), containsString("is reserved"));

        final Error allowed = Roles.validateRoleName(rolename, true, CUSTOM_MAX_NAME_LENGTH);
        assertNull(allowed);
    }

    public void testRoleNameInvalidLengthWithCustomLength() throws Exception {
        int length = frequently() ? randomIntBetween(CUSTOM_MAX_NAME_LENGTH + 1, CUSTOM_MAX_NAME_LENGTH * 2) : 0;
        char[] name = new char[length];
        if (length > 0) {
            name = generateValidName(length);
        }
        assertThat(Roles.validateRoleName(new String(name), false, CUSTOM_MAX_NAME_LENGTH), notNullValue());
    }

    public void testRoleNameInvalidCharactersWithCustomLength() throws Exception {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, CUSTOM_MAX_NAME_LENGTH);
        String name = new String(generateNameInvalidCharacters(length));
        assertThat(Roles.validateRoleName(name, false, CUSTOM_MAX_NAME_LENGTH), notNullValue());
    }

    public void testRoleNameInvalidWhitespaceWithCustomLength() throws Exception {
        int length = randomIntBetween(Validation.MIN_NAME_LENGTH, CUSTOM_MAX_NAME_LENGTH);
        String name = new String(generateNameInvalidWhitespace(length));
        assertThat(Roles.validateRoleName(name, false, CUSTOM_MAX_NAME_LENGTH), notNullValue());
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

    public void testUserManagedServiceAccountIdValid() {
        final String namespace = randomUserManagedComponent();
        final String serviceName = randomUserManagedComponent();
        assertThat(UserManagedServiceAccounts.validateNamespace(namespace), nullValue());
        assertThat(UserManagedServiceAccounts.validateServiceName(serviceName), nullValue());
        assertThat(UserManagedServiceAccounts.validatePrincipal(namespace + "/" + serviceName), nullValue());
    }

    public void testUserManagedServiceAccountIdRejectsBuiltInNamespace() {
        final String reserved = randomFrom("elastic", "ELASTIC", "Elastic", "eLaStIc");
        assertThat(
            UserManagedServiceAccounts.validateNamespace(reserved).toString(),
            containsString("the [elastic] namespace is reserved for built-in service accounts")
        );
        assertThat(
            UserManagedServiceAccounts.validatePrincipal(reserved + "/" + randomUserManagedComponent()).toString(),
            containsString("reserved for built-in service accounts")
        );
        // Prefixes of the reserved name are a different namespace
        assertThat(UserManagedServiceAccounts.validateNamespace("elastic-team"), nullValue());
        assertThat(UserManagedServiceAccounts.validateServiceName("elastic"), nullValue());
    }

    public void testUserManagedServiceAccountIdRejectsMissingComponents() {
        final String empty = randomFrom("", null);
        assertThat(UserManagedServiceAccounts.validateNamespace(empty).toString(), containsString("service account namespace is missing"));
        assertThat(
            UserManagedServiceAccounts.validateServiceName(empty).toString(),
            containsString("service account service name is missing")
        );
        assertThat(UserManagedServiceAccounts.validatePrincipal(null).toString(), containsString("principal is missing"));
    }

    public void testUserManagedServiceAccountIdRejectsMalformedPrincipal() {
        final String noSeparator = randomUserManagedComponent();
        assertThat(
            UserManagedServiceAccounts.validatePrincipal(noSeparator).toString(),
            containsString("a service account ID must be in the form {namespace}/{service-name}, but was [" + noSeparator + "]")
        );
        // Splitting on the first separator leaves any extra one in the service name, where the charset rejects it
        assertThat(
            UserManagedServiceAccounts.validatePrincipal("a/b/c").toString(),
            containsString("service account service name [b/c] must begin with a letter or digit")
        );
    }

    public void testUserManagedServiceAccountIdRejectsInvalidCharacters() {
        final String component = randomUserManagedComponent();
        // '/' separates the two components of a principal and ':' separates a principal from a token secret, so both
        // must be rejected however they appear
        for (String invalid : List.of(component + "/x", component + ":x", " " + component, component + " ", component + "\t")) {
            assertThat(
                "expected [" + invalid + "] to be rejected",
                UserManagedServiceAccounts.validateNamespace(invalid).toString(),
                containsString("must begin with a letter or digit and may contain only letters, digits, hyphens (-) and underscores (_)")
            );
        }
        // Hyphens and underscores are allowed, but not as the first character
        assertThat(UserManagedServiceAccounts.validateServiceName("a-b_c"), nullValue());
        assertThat(UserManagedServiceAccounts.validateServiceName(randomFrom("-a", "_a")), notNullValue());
    }

    public void testUserManagedServiceAccountIdLengthBoundary() {
        final int max = UserManagedServiceAccounts.MAX_COMPONENT_LENGTH;
        assertThat(UserManagedServiceAccounts.validateNamespace(randomAlphaOfLength(max)), nullValue());
        assertThat(UserManagedServiceAccounts.validateServiceName(randomAlphaOfLength(max)), nullValue());

        final String tooLong = randomAlphaOfLengthBetween(max + 1, max + 20);
        assertThat(
            UserManagedServiceAccounts.validateNamespace(tooLong).toString(),
            containsString("service account namespace may not be more than " + max + " characters long, but [" + tooLong + "] is ")
        );
        assertThat(UserManagedServiceAccounts.validatePrincipal(randomAlphaOfLength(max) + "/" + tooLong), notNullValue());
    }

    private static String randomUserManagedComponent() {
        return randomAlphaOfLength(1) + randomFrom("-", "_") + randomFrom("", randomAlphaOfLengthBetween(1, 20));
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
        final Character[] chars = randomArray(1, 256, Character[]::new, () -> randomFrom(VALID_SERVICE_ACCOUNT_TOKEN_NAME_CHARS));
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
