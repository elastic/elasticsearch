/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.common.secret;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.crypto.CryptoService;

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

        @Inject
        public Secure(Settings settings, CryptoService cryptoService) {
            super(settings);
            this.cryptoService = cryptoService;
        }

        @Override
        public char[] encrypt(char[] text) {
            return cryptoService.encrypt(text);
        }

        @Override
        public char[] decrypt(char[] text) {
            return cryptoService.decrypt(text);
        }
    }
}
