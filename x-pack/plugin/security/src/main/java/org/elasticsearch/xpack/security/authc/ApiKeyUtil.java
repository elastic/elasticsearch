/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CharArrays;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

public class ApiKeyUtil {

    public static final char[] COLON_CHARS = ":".toCharArray();
    public static final int COLON_CHARS_LENGTH = COLON_CHARS.length;
    public static final char COLON_CHAR = COLON_CHARS[0];

    public static SecureString toBase64(final ApiKeyService.ApiKeyCredentials apiKeyCredentials) throws IOException {
        return toBase64(apiKeyCredentials.getId(), apiKeyCredentials.getKey());
    }

    public static ApiKeyService.ApiKeyCredentials toApiKeyCredentials(final String id, final SecureString key) {
        return new ApiKeyService.ApiKeyCredentials(id, key);
    }

    public static SecureString toBase64(final String id, final SecureString key) throws IOException {
        final char[] idChars = id.toCharArray();
        final char[] keyChars = key.getChars(); // direct reference, caller is responsible for clearing it
        final int idCharsLength = idChars.length;
        final int keyCharsLength = keyChars.length;
        char[] concatenatedChars = null;
        byte[] concatenatedBytes = null;
        byte[] base64Bytes = null;
        char[] base64CharArray = null;
        try {
            concatenatedChars = new char[idCharsLength + COLON_CHARS_LENGTH + keyCharsLength];
            System.arraycopy(idChars, 0, concatenatedChars, 0, idCharsLength);
            System.arraycopy(COLON_CHARS, 0, concatenatedChars, idCharsLength, COLON_CHARS_LENGTH);
            System.arraycopy(keyChars, 0, concatenatedChars, idCharsLength + COLON_CHARS_LENGTH, keyCharsLength);

            concatenatedBytes = StandardCharsets.UTF_8.encode(CharBuffer.wrap(concatenatedChars)).array(); // direct reference
            base64Bytes = Base64.getEncoder().encode(concatenatedBytes);
            base64CharArray = StandardCharsets.UTF_8.decode(ByteBuffer.wrap(base64Bytes)).array(); // direct reference
            return new SecureString(base64CharArray);
        } finally {
            if (concatenatedChars != null) {
                Arrays.fill(concatenatedChars, COLON_CHAR);
            }
            if (concatenatedBytes != null) {
                Arrays.fill(concatenatedBytes, (byte) 0);
            }
            if (base64Bytes != null) {
                Arrays.fill(base64Bytes, (byte) 0);
            }
            if (base64CharArray != null) {
                Arrays.fill(base64CharArray, COLON_CHAR);
            }
        }
    }

    public static ApiKeyService.ApiKeyCredentials toApiKeyCredentials(SecureString encoded) {
        if (encoded != null) {
            final byte[] decodedApiKeyCredBytes = Base64.getDecoder().decode(CharArrays.toUtf8Bytes(encoded.getChars()));
            char[] apiKeyCredChars = null;
            try {
                apiKeyCredChars = CharArrays.utf8BytesToChars(decodedApiKeyCredBytes);
                int colonIndex = -1;
                for (int i = 0; i < apiKeyCredChars.length; i++) {
                    if (apiKeyCredChars[i] == ':') {
                        colonIndex = i;
                        break;
                    }
                }

                if (colonIndex < 1) {
                    throw new IllegalArgumentException("invalid ApiKey value");
                }
                return new ApiKeyService.ApiKeyCredentials(
                    new String(Arrays.copyOfRange(apiKeyCredChars, 0, colonIndex)),
                    new SecureString(Arrays.copyOfRange(apiKeyCredChars, colonIndex + 1, apiKeyCredChars.length))
                );
            } finally {
                if (apiKeyCredChars != null) {
                    Arrays.fill(apiKeyCredChars, (char) 0);
                }
            }
        }
        return null;
    }
}
