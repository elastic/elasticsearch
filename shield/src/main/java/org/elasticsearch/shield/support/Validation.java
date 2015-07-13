/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support;

import java.util.regex.Pattern;

/**
 *
 */
public final class Validation {

    private static final Pattern COMMON_NAME_PATTERN = Pattern.compile("[a-zA-Z_][a-zA-Z0-9_@\\-\\$\\.]{0,29}");

    public static final class ESUsers {

        private static final int MIN_PASSWD_LENGTH = 6;

        public static Error validateUsername(String username) {
            return COMMON_NAME_PATTERN.matcher(username).matches() ?
                    null :
                    new Error("A valid username must be at least 1 character and no longer than 30 characters. " +
                            "It must begin with a letter (`a-z` or `A-Z`) or an underscore (`_`). Subsequent " +
                            "characters can be letters, underscores (`_`), digits (`0-9`) or any of the following " +
                            "symbols `@`, `-`, `.` or `$`");
        }

        public static Error validatePassword(char[] password) {
            return password.length >= MIN_PASSWD_LENGTH ?
                    null :
                    new Error("passwords must be at least [" + MIN_PASSWD_LENGTH + "] characters long");
        }

    }

    public static final class Roles {

        public static Error validateRoleName(String roleName) {
            return COMMON_NAME_PATTERN.matcher(roleName).matches() ?
                    null :
                    new Error("A valid role name must be at least 1 character and no longer than 30 characters. " +
                            "It must begin with a letter (`a-z` or `A-Z`) or an underscore (`_`). Subsequent " +
                            "characters can be letters, underscores (`_`), digits (`0-9`) or any of the following " +
                            "symbols `@`, `-`, `.` or `$`");
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
