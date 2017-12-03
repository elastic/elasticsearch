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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.util.IOUtils;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

public class KeyStoreWrapperTests extends ESTestCase {

    Environment env;
    List<FileSystem> fileSystems = new ArrayList<>();

    @After
    public void closeMockFileSystems() throws IOException {
        IOUtils.close(fileSystems);
    }

    @Before
    public void setupEnv() throws IOException {
        env = KeyStoreCommandTestCase.setupEnv(true, fileSystems);
    }

    public void testFileSettingExhaustiveBytes() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create(new char[0]);
        byte[] bytes = new byte[256];
        for (int i = 0; i < 256; ++i) {
            bytes[i] = (byte)i;
        }
        keystore.setFile("foo", bytes);
        keystore.save(env.configFile());
        keystore = KeyStoreWrapper.load(env.configFile());
        keystore.decrypt(new char[0]);
        try (InputStream stream = keystore.getFile("foo")) {
            for (int i = 0; i < 256; ++i) {
                int got = stream.read();
                if (got < 0) {
                    fail("Expected 256 bytes but read " + i);
                }
                assertEquals(i, got);
            }
            assertEquals(-1, stream.read()); // nothing left
        }
    }

    public void testCreate() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create(new char[0]);
        assertTrue(keystore.getSettingNames().contains(KeyStoreWrapper.SEED_SETTING.getKey()));
    }

    public void testUpgradeNoop() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create(new char[0]);
        SecureString seed = keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey());
        keystore.save(env.configFile());
        // upgrade does not overwrite seed
        KeyStoreWrapper.upgrade(keystore, env.configFile());
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
        keystore = KeyStoreWrapper.load(env.configFile());
        keystore.decrypt(new char[0]);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
    }

    public void testUpgradeAddsSeed() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create(new char[0]);
        keystore.remove(KeyStoreWrapper.SEED_SETTING.getKey());
        keystore.save(env.configFile());
        KeyStoreWrapper.upgrade(keystore, env.configFile());
        SecureString seed = keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey());
        assertNotNull(seed);
        keystore = KeyStoreWrapper.load(env.configFile());
        keystore.decrypt(new char[0]);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
    }
}
