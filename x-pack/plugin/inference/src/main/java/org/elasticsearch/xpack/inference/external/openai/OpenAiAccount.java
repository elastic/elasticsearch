/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.elasticsearch.common.settings.SecureString;

public class OpenAiAccount {
    // TODO this should take the service settings and store save the encrypted key in memory
    // and should take the crypto service and provide a method to return the api key

    // TODO maybe this should be a SecureString? or a char[]?
    private final String encryptedApiKey;

    public OpenAiAccount(String encryptedApiKey) {
        this.encryptedApiKey = encryptedApiKey;
    }

    public SecureString getApiKey() {
        // TODO normally we'd do the decryption here
        return new SecureString(this.encryptedApiKey.toCharArray());
    }
}
