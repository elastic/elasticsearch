/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.tool;

import org.elasticsearch.core.Nullable;

import java.security.SecureRandom;

public class CommandUtils {

    static final SecureRandom SECURE_RANDOM = new SecureRandom();

    /**
     * Generates a password of a given length from a set of predefined allowed chars.
     * @param passwordLength the length of the password
     * @return the char array with the password
     */
    public static char[] generatePassword(int passwordLength) {
        final char[] passwordChars = ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789*-_=+").toCharArray();
        char[] characters = new char[passwordLength];
        for (int i = 0; i < passwordLength; ++i) {
            characters[i] = passwordChars[SECURE_RANDOM.nextInt(passwordChars.length)];
        }
        return characters;
    }

    /**
     * Generates a string that can be used as a username, possibly consisting of a chosen prefix and suffix
     */
    protected static String generateUsername(@Nullable String prefix, @Nullable String suffix, int length) {
        final char[] usernameChars = ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789").toCharArray();

        final String prefixString = null == prefix ? "" : prefix;
        final String suffixString = null == suffix ? "" : prefix;
        char[] characters = new char[length];
        for (int i = 0; i < length; ++i) {
            characters[i] = usernameChars[SECURE_RANDOM.nextInt(usernameChars.length)];
        }
        return prefixString + new String(characters) + suffixString;
    }
}
