/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.groq;

import org.apache.http.Header;
import org.apache.http.message.BasicHeader;

public final class GroqUtils {

    public static final String HOST = "api.groq.com";
    public static final String OPENAI_PATH = "openai";
    public static final String VERSION_1 = "v1";
    public static final String CHAT_PATH = "chat";
    public static final String COMPLETIONS_PATH = "completions";

    private static final String ORGANIZATION_HEADER = "Groq-Organization";

    public static Header createOrgHeader(String org) {
        return new BasicHeader(ORGANIZATION_HEADER, org);
    }

    private GroqUtils() {}
}
