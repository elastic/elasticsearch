/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic;

public class AnthropicServiceFields {

    public static final String MAX_TOKENS = "max_tokens";
    public static final String TEMPERATURE_FIELD = "temperature";
    public static final String TOP_P_FIELD = "top_p";
    public static final String TOP_K_FIELD = "top_k";

    /**
     * The format of Claude reasoning details, shared by every service that streams Claude reasoning so clients can replay it to Claude.
     */
    public static final String ANTHROPIC_CLAUDE_V1_FORMAT = "anthropic-claude-v1";
}
