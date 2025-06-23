/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.CharArrays;

import java.util.Arrays;
import java.util.Base64;

public final class SecureRandomUtils {
    private SecureRandomUtils() {}

    /**
     * Returns a cryptographically secure Base64 encoded {@link SecureString} of {@code numBytes} random bytes.
     */
    public static SecureString getBase64SecureRandomString(int numBytes) {
        byte[] randomBytes = null;
        byte[] encodedBytes = null;
        try {
            randomBytes = new byte[numBytes];
            SecureRandomHolder.INSTANCE.nextBytes(randomBytes);
            encodedBytes = Base64.getUrlEncoder().withoutPadding().encode(randomBytes);
            return new SecureString(CharArrays.utf8BytesToChars(encodedBytes));
        } finally {
            if (randomBytes != null) {
                Arrays.fill(randomBytes, (byte) 0);
            }
            if (encodedBytes != null) {
                Arrays.fill(encodedBytes, (byte) 0);
            }
        }
    }
}
