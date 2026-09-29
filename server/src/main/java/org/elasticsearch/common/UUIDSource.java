/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

import java.util.OptionalInt;

/**
 * Generates the time-based IDs returned by {@link UUIDs}. Loaded through SPI so the test framework can supply a seeded implementation.
 */
public interface UUIDSource {

    /**
     * Returns a unique, URL-safe base64 ID without padding, {@link UUIDs#TIME_BASED_UUID_STRING_LENGTH} characters long.
     */
    String base64UUID();

    /**
     * Returns a unique, URL-safe base64 ID without padding that decodes to 15 bytes, or 19 bytes when {@code hash} is present.
     * The hash is stored little-endian at decoded offset 10, where routing reads it back to locate the shard.
     */
    String base64TimeBasedKOrderedUUIDWithHash(OptionalInt hash);
}
