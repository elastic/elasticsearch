/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;

/**
 * POJO containing the keystore and its password
 */
final class KeystoreAndPassword implements Closeable {
    private final KeyStoreWrapper keystore;
    private final char[] password;

    KeystoreAndPassword(KeyStoreWrapper keystore, char[] password) {
        this.keystore = keystore;
        this.password = password;
    }

    public KeyStoreWrapper getKeystore() {
        return keystore;
    }

    public char[] getPassword() {
        return password;
    }

    @Override
    public void close() throws IOException {
        if (null != password) {
            Arrays.fill(password, '\u0000');
        }
    }
}
