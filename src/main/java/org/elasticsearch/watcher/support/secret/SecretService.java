/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.secret;

/**
 *
 */
public interface SecretService {

    char[] encrypt(char[] text);

    char[] decrypt(char[] text);

    class PlainText implements SecretService {

        @Override
        public char[] encrypt(char[] text) {
            return text;
        }

        @Override
        public char[] decrypt(char[] text) {
            return text;
        }
    }
}
