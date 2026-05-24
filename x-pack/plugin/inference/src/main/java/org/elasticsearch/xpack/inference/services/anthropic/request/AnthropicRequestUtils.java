/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.request;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.common.settings.SecureString;

public class AnthropicRequestUtils {
    public static final String HOST = "api.anthropic.com";
    public static final String API_VERSION_1 = "v1";
    public static final String MESSAGES_PATH = "messages";

    public static final String ANTHROPIC_VERSION_2023_06_01 = "2023-06-01";

    public static final String X_API_KEY = "x-api-key";
    public static final String VERSION = "anthropic-version";

    public static Header createAuthBearerHeader(SecureString apiKey) {
        return new BasicHeader(X_API_KEY, apiKey.toString());
    }

    public static Header createVersionHeader() {
        return new BasicHeader(VERSION, ANTHROPIC_VERSION_2023_06_01);
    }

    private AnthropicRequestUtils() {}
}
