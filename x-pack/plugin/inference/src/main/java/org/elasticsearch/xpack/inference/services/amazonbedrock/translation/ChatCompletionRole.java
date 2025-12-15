/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock.translation;

import java.util.Locale;

public enum ChatCompletionRole {
    SYSTEM,
    USER,
    ASSISTANT,
    TOOL;

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }

    public static ChatCompletionRole fromString(String name) {
        try {
            return valueOf(name.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("Received invalid role [%s]", name), e);
        }
    }
}
