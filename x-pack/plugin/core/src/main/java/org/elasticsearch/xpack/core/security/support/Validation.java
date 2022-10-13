/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.security.authc.esnative.ClientReservedRealm;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;

import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

public final class Validation {

    static final int MIN_NAME_LENGTH = 1;
    static final int MAX_NAME_LENGTH = 1024;

    public static final Set<Character> VALID_NAME_CHARS = Set.of(
        ' ',
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
        '-',
        '.',
        '/',
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
        ':',
        ';',
        '<',
        '=',
        '>',
        '?',
        '@',
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
        '[',
        '\\',
        ']',
        '^',
        '_',
        '`',
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
        '{',
        '|',
        '}',
        '~'
    );

    private static final String INVALID_NAME_MESSAGE = "%1s names must be at least "
        + MIN_NAME_LENGTH
        + " and no more than %s characters. "
        + "They can contain alphanumeric characters (a-z, A-Z, 0-9), spaces, punctuation, and printable symbols in the "
        + "Basic Latin (ASCII) block. Leading or trailing whitespace is not allowed.";

    private static final Pattern VALID_SERVICE_ACCOUNT_TOKEN_NAME = Pattern.compile("^[a-zA-Z0-9-][a-zA-Z0-9_-]{0,255}$");

    public static final String INVALID_SERVICE_ACCOUNT_TOKEN_NAME_MESSAGE = "service account token name must have at least 1 character "
        + "and at most 256 characters that are alphanumeric (A-Z, a-z, 0-9) or hyphen (-) or underscore (_). "
        + "It must not begin with an underscore (_).";

    private static boolean isValidUserOrRoleName(String name, int maxLength) {
        assert maxLength >= MIN_NAME_LENGTH : "maxLength cannot be less than " + MIN_NAME_LENGTH;

        if (name.length() < MIN_NAME_LENGTH || name.length() > maxLength) {
            return false;
        }

        for (char character : name.toCharArray()) {
            if (VALID_NAME_CHARS.contains(character) == false) {
                return false;
            }
        }

        // We only check against the space character here (U+0020) since it's the only whitespace character in the
        // set that we allow.
        //
        // Note for the future if we allow the full unicode range: the String and Character methods that deal with
        // whitespace don't work for the whole range. They match characters that are considered whitespace to the Java
        // language, which doesn't include characters like IDEOGRAPHIC SPACE (U+3000). The best approach here may be
        // to match against java.util.regex.Pattern's \p{Space} class (which is by default broader than \s) or make a
        // list from the codepoints listed in this page https://en.wikipedia.org/wiki/Whitespace_character
        if (name.startsWith(" ") || name.endsWith(" ")) {
            return false;
        }

        return true;
    }

    public static boolean isValidServiceAccountTokenName(String name) {
        return name != null && VALID_SERVICE_ACCOUNT_TOKEN_NAME.matcher(name).matches();
    }

    public static String formatInvalidServiceTokenNameErrorMessage(String name) {
        return "invalid service token name [" + name + "]. " + INVALID_SERVICE_ACCOUNT_TOKEN_NAME_MESSAGE;
    }

    public static final class Users {

        private static final int MIN_PASSWD_LENGTH = 6;

        /**
         * Validate the username
         * @param username the username to validate
         * @param allowReserved whether or not to allow reserved user names
         * @param settings the settings which may contain information about reserved users
         * @return {@code null} if valid
         */
        public static Error validateUsername(String username, boolean allowReserved, Settings settings) {
            return validateUsername(username, allowReserved, settings, MAX_NAME_LENGTH);
        }

        static Error validateUsername(String username, boolean allowReserved, Settings settings, int maxLength) {
            if (username == null) {
                return new Error("username is missing");
            }
            if (isValidUserOrRoleName(username, maxLength) == false) {
                return new Error(String.format(Locale.ROOT, INVALID_NAME_MESSAGE, "User", maxLength));
            }
            if (allowReserved == false && ClientReservedRealm.isReserved(username, settings)) {
                return new Error("Username [" + username + "] is reserved and may not be used.");
            }
            return null;
        }

        public static Error validatePassword(SecureString password) {
            return password.length() >= MIN_PASSWD_LENGTH
                ? null
                : new Error("passwords must be at least [" + MIN_PASSWD_LENGTH + "] characters long");
        }

    }

    public static final class Roles {

        public static Error validateRoleName(String roleName, boolean allowReserved) {
            return validateRoleName(roleName, allowReserved, MAX_NAME_LENGTH);
        }

        static Error validateRoleName(String roleName, boolean allowReserved, int maxLength) {
            if (roleName == null) {
                return new Error("role name is missing");
            }
            if (isValidUserOrRoleName(roleName, maxLength) == false) {
                return new Error(String.format(Locale.ROOT, INVALID_NAME_MESSAGE, "Role", maxLength));
            }
            if (allowReserved == false && ReservedRolesStore.isReserved(roleName)) {
                return new Error("Role [" + roleName + "] is reserved and may not be used.");
            }
            return null;
        }
    }

    public static class Error {

        private final String message;

        private Error(String message) {
            this.message = message;
        }

        @Override
        public String toString() {
            return message;
        }
    }
}
