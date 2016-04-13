/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.secret;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.shield.crypto.CryptoService;

/**
 *
 */
public interface SecretService {

    char[] encrypt(char[] text);

    char[] decrypt(char[] text);

    class Insecure implements SecretService {

        public static final Insecure INSTANCE = new Insecure();

        Insecure() {
        }

        @Override
        public char[] encrypt(char[] text) {
            return text;
        }

        @Override
        public char[] decrypt(char[] text) {
            return text;
        }
    }

    /**
     *
     */
    class Secure extends AbstractComponent implements SecretService {

        private final CryptoService cryptoService;
        private final boolean encryptSensitiveData;
        public static final Setting<Boolean> ENCRYPT_SENSITIVE_DATA_SETTING =
                Setting.boolSetting("xpack.watcher.shield.encrypt_sensitive_data", false, Setting.Property.NodeScope);
        @Inject
        public Secure(Settings settings, CryptoService cryptoService) {
            super(settings);
            this.encryptSensitiveData = ENCRYPT_SENSITIVE_DATA_SETTING.get(settings);
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
}
