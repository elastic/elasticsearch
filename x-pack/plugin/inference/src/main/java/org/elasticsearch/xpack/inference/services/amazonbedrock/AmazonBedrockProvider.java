/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.amazonbedrock;

import java.util.Locale;

public enum AmazonBedrockProvider {
    AMAZONTITAN,
    ANTHROPIC,
    AI21LABS,
    COHERE,
    META,
    MISTRAL;

    public static String NAME = "amazon_bedrock_provider";

    public static AmazonBedrockProvider fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
