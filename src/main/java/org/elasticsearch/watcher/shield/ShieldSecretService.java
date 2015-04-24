/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.shield;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.crypto.CryptoService;
import org.elasticsearch.watcher.support.secret.SecretService;

/**
 *
 */
public class ShieldSecretService extends AbstractComponent implements SecretService {

    private final CryptoService cryptoService;
    private final boolean encryptSensitiveData;

    @Inject
    public ShieldSecretService(Settings settings, CryptoService cryptoService) {
        super(settings);
        this.encryptSensitiveData = componentSettings.getAsBoolean("encrypt_sensitive_data", false);
        this.cryptoService = cryptoService;
    }

    @Override
    public char[] encrypt(char[] text) {
        return encryptSensitiveData ? cryptoService.encrypt(text) : text;
    }

    @Override
    public char[] decrypt(char[] text) {
        return encryptSensitiveData ? cryptoService.decrypt(text) : text;
    }
}
