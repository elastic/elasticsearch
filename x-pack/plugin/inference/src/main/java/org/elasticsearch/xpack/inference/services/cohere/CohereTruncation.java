/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere;

import java.util.EnumSet;
import java.util.Locale;

/**
 * Defines the type of truncation for a cohere request. The specified value determines how the Cohere API will handle inputs
 * longer than the maximum token length.
 *
 * <p>
 * <a href="https://docs.cohere.com/reference/embed">See api docs for details.</a>
 * </p>
 */
public enum CohereTruncation {
    /**
     * When the input exceeds the maximum input token length an error will be returned.
     */
    NONE,
    /**
     * Discard the start of the input
     */
    START,
    /**
     * Discard the end of the input
     */
    END;

    public static final EnumSet<CohereTruncation> ALL = EnumSet.allOf(CohereTruncation.class);

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static CohereTruncation fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }
}
