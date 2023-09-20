/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.elasticsearch.common.settings.SecureString;

public class OpenAiAccount {
    private final SecureString encryptedApiKey;

    public OpenAiAccount(SecureString encryptedApiKey) {
        this.encryptedApiKey = encryptedApiKey;
    }

    public SecureString getApiKey() {
        // TODO normally we'd do the decryption here
        return encryptedApiKey;
    }
}
