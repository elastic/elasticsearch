/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common;

import org.elasticsearch.common.settings.SecureString;

import java.util.Random;

public class UUIDs {

    private static final RandomBasedUUIDGenerator RANDOM_UUID_GENERATOR = new RandomBasedUUIDGenerator();
    private static final UUIDGenerator TIME_UUID_GENERATOR = new TimeBasedUUIDGenerator();

    /** Generates a time-based UUID (similar to Flake IDs), which is preferred when generating an ID to be indexed into a Lucene index as
     *  primary key.  The id is opaque and the implementation is free to change at any time! */
    public static String base64UUID() {
        return TIME_UUID_GENERATOR.getBase64UUID();
    }

    /** Returns a Base64 encoded version of a Version 4.0 compatible UUID as defined here: http://www.ietf.org/rfc/rfc4122.txt, using the
     *  provided {@code Random} instance */
    public static String randomBase64UUID(Random random) {
        return RandomBasedUUIDGenerator.getBase64UUID(random);
    }

    /** Returns a Base64 encoded version of a Version 4.0 compatible UUID as defined here: http://www.ietf.org/rfc/rfc4122.txt, using a
     *  private {@code SecureRandom} instance */
    public static String randomBase64UUID() {
        return RANDOM_UUID_GENERATOR.getBase64UUID();
    }

    /** Returns a Base64 encoded {@link SecureString} of a Version 4.0 compatible UUID as defined here: http://www.ietf.org/rfc/rfc4122.txt,
     *  using a private {@code SecureRandom} instance */
    public static SecureString randomBase64UUIDSecureString() {
        return RandomBasedUUIDGenerator.getBase64UUIDSecureString();
    }
}
