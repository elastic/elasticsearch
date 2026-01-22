/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ai21.request;

/**
 * Constants for AI21 API endpoints and paths.
 * This class defines the base host, version, and specific paths used in AI21 API requests.
 */
public class Ai21ApiConstants {
    public static final String HOST = "api.ai21.com";
    public static final String VERSION_1 = "v1";
    public static final String STUDIO_PATH = "studio";
    public static final String CHAT_PATH = "chat";
    public static final String COMPLETIONS_PATH = "completions";

    private Ai21ApiConstants() {}
}
