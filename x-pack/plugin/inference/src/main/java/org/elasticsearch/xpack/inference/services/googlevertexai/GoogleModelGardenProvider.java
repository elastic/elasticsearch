/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai;

import java.util.Locale;

/**
 * Enum representing the supported model garden providers.
 */
public enum GoogleModelGardenProvider {
    GOOGLE,
    ANTHROPIC;

    public static final String NAME = "google_model_garden_provider";

    public static GoogleModelGardenProvider fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
