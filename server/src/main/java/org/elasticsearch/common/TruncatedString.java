/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common;

/**
 * A small value class that represents a {@link String} that might be truncated.
 *
 * @param string The string value.
 * @param truncated {@code true} if the string was truncated, {@code false} otherwise.
 */
public record TruncatedString(String string, boolean truncated) {

    /**
     * Appends a given {@code suffix} and returns the result if the value if {@link #string} was truncated; or the unchanged {@link #string}
     * otherwise.
     *
     * @param suffix Suffix to append.
     */
    public String appendIfTruncated(String suffix) {
        return truncated ? string + suffix : string;
    }
}
