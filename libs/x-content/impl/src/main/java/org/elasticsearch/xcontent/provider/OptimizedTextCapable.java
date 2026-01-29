/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent.provider;

import org.elasticsearch.xcontent.Text;

import java.io.IOException;

/**
 * Indicates that a {@link com.fasterxml.jackson.core.JsonParser} is capable of
 * returning the underlying UTF-8 encoded bytes of the current string token.
 * This is useful for performance optimizations, as it allows the parser to
 * avoid unnecessary conversions to and from strings.
 */
public interface OptimizedTextCapable {

    /**
     * Method that will try to get underlying UTF-8 encoded bytes of the current string token.
     * This is only a best-effort attempt; if there is some reason the bytes cannot be retrieved, this method will return null.
     */
    Text getValueAsText() throws IOException;
}
