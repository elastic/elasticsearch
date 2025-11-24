/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.model;

import java.util.EnumSet;
import java.util.Locale;

/**
 * Defines the type of truncation for an embeddings request. The specified value determines how the provider's API will handle inputs
 * longer than the maximum token length.
 * <p>
 * <a href="https://docs.cohere.com/reference/embed">Details can be found in Cohere embeddings API docs.</a>
 * </p>
 */
public enum Truncation {
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

    public static final EnumSet<Truncation> ALL = EnumSet.allOf(Truncation.class);

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static Truncation fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }
}
