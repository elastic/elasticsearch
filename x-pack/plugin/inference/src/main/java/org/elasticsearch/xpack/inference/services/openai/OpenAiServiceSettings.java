/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openai;

/**
 * Common surface implemented by OpenAI service settings classes that may carry
 * OAuth2 configuration. Exists so the secrets factory and applier can read
 * OAuth2 settings without depending on a specific task-type settings subclass.
 */
public interface OpenAiServiceSettings {
    OpenAiOAuth2Settings oAuth2Settings();
}
