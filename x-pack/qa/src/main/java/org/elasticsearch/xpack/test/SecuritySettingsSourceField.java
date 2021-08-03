/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.test;

import org.elasticsearch.core.CharArrays;
import org.elasticsearch.common.settings.SecureString;

import java.nio.CharBuffer;
import java.util.Arrays;
import java.util.Base64;

public final class SecuritySettingsSourceField {
    public static final SecureString TEST_PASSWORD_SECURE_STRING = new SecureString("x-pack-test-password".toCharArray());
    public static final String TEST_PASSWORD = "x-pack-test-password";

    private SecuritySettingsSourceField() {}

    public static String basicAuthHeaderValue(String username, String passwd) {
        return basicAuthHeaderValue(username, new SecureString(passwd.toCharArray()));
    }

    public static String basicAuthHeaderValue(String username, SecureString passwd) {
        CharBuffer chars = CharBuffer.allocate(username.length() + passwd.length() + 1);
        byte[] charBytes = null;
        try {
            chars.put(username).put(':').put(passwd.getChars());
            charBytes = CharArrays.toUtf8Bytes(chars.array());

            //TODO we still have passwords in Strings in headers. Maybe we can look into using a CharSequence?
            String basicToken = Base64.getEncoder().encodeToString(charBytes);
            return "Basic " + basicToken;
        } finally {
            Arrays.fill(chars.array(), (char) 0);
            if (charBytes != null) {
                Arrays.fill(charBytes, (byte) 0);
            }
        }
    }
}
