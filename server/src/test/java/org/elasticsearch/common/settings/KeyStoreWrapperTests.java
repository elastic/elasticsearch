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

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;

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
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        byte[] bytes = new byte[256];
        for (int i = 0; i < 256; ++i) {
            bytes[i] = (byte)i;
        }
        keystore.setFile("foo", bytes);
        keystore.save(env.configFile(), new char[0]);
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
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        assertTrue(keystore.getSettingNames().contains(KeyStoreWrapper.SEED_SETTING.getKey()));
    }

    public void testUpgradeNoop() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        SecureString seed = keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey());
        keystore.save(env.configFile(), new char[0]);
        // upgrade does not overwrite seed
        KeyStoreWrapper.upgrade(keystore, env.configFile(), new char[0]);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
        keystore = KeyStoreWrapper.load(env.configFile());
        keystore.decrypt(new char[0]);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
    }

    public void testUpgradeAddsSeed() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        keystore.remove(KeyStoreWrapper.SEED_SETTING.getKey());
        keystore.save(env.configFile(), new char[0]);
        KeyStoreWrapper.upgrade(keystore, env.configFile(), new char[0]);
        SecureString seed = keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey());
        assertNotNull(seed);
        keystore = KeyStoreWrapper.load(env.configFile());
        keystore.decrypt(new char[0]);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
    }

    public void testIllegalSettingName() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> KeyStoreWrapper.validateSettingName("UpperCase"));
        assertTrue(e.getMessage().contains("does not match the allowed setting name pattern"));
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        e = expectThrows(IllegalArgumentException.class, () -> keystore.setString("UpperCase", new char[0]));
        assertTrue(e.getMessage().contains("does not match the allowed setting name pattern"));
        e = expectThrows(IllegalArgumentException.class, () -> keystore.setFile("UpperCase", new byte[0]));
        assertTrue(e.getMessage().contains("does not match the allowed setting name pattern"));
    }

    public void testBackcompatV1() throws Exception {
        Path configDir = env.configFile();
        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        try (IndexOutput output = directory.createOutput("elasticsearch.keystore", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(output, "elasticsearch.keystore", 1);
            output.writeByte((byte) 0); // hasPassword = false
            output.writeString("PKCS12");
            output.writeString("PBE");

            SecretKeyFactory secretFactory = SecretKeyFactory.getInstance("PBE");
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            keystore.load(null, null);
            SecretKey secretKey = secretFactory.generateSecret(new PBEKeySpec("stringSecretValue".toCharArray()));
            KeyStore.ProtectionParameter protectionParameter = new KeyStore.PasswordProtection(new char[0]);
            keystore.setEntry("string_setting", new KeyStore.SecretKeyEntry(secretKey), protectionParameter);

            ByteArrayOutputStream keystoreBytesStream = new ByteArrayOutputStream();
            keystore.store(keystoreBytesStream, new char[0]);
            byte[] keystoreBytes = keystoreBytesStream.toByteArray();
            output.writeInt(keystoreBytes.length);
            output.writeBytes(keystoreBytes, keystoreBytes.length);
            CodecUtil.writeFooter(output);
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        keystore.decrypt(new char[0]);
        SecureString testValue = keystore.getString("string_setting");
        assertThat(testValue.toString(), equalTo("stringSecretValue"));
    }

    public void testBackcompatV2() throws Exception {
        Path configDir = env.configFile();
        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        byte[] fileBytes = new byte[20];
        random().nextBytes(fileBytes);
        try (IndexOutput output = directory.createOutput("elasticsearch.keystore", IOContext.DEFAULT)) {

            CodecUtil.writeHeader(output, "elasticsearch.keystore", 2);
            output.writeByte((byte) 0); // hasPassword = false
            output.writeString("PKCS12");
            output.writeString("PBE"); // string algo
            output.writeString("PBE"); // file algo

            output.writeVInt(2); // num settings
            output.writeString("string_setting");
            output.writeString("STRING");
            output.writeString("file_setting");
            output.writeString("FILE");

            SecretKeyFactory secretFactory = SecretKeyFactory.getInstance("PBE");
            KeyStore keystore = KeyStore.getInstance("PKCS12");
            keystore.load(null, null);
            SecretKey secretKey = secretFactory.generateSecret(new PBEKeySpec("stringSecretValue".toCharArray()));
            KeyStore.ProtectionParameter protectionParameter = new KeyStore.PasswordProtection(new char[0]);
            keystore.setEntry("string_setting", new KeyStore.SecretKeyEntry(secretKey), protectionParameter);

            byte[] base64Bytes = Base64.getEncoder().encode(fileBytes);
            char[] chars = new char[base64Bytes.length];
            for (int i = 0; i < chars.length; ++i) {
                chars[i] = (char)base64Bytes[i]; // PBE only stores the lower 8 bits, so this narrowing is ok
            }
            secretKey = secretFactory.generateSecret(new PBEKeySpec(chars));
            keystore.setEntry("file_setting", new KeyStore.SecretKeyEntry(secretKey), protectionParameter);

            ByteArrayOutputStream keystoreBytesStream = new ByteArrayOutputStream();
            keystore.store(keystoreBytesStream, new char[0]);
            byte[] keystoreBytes = keystoreBytesStream.toByteArray();
            output.writeInt(keystoreBytes.length);
            output.writeBytes(keystoreBytes, keystoreBytes.length);
            CodecUtil.writeFooter(output);
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        keystore.decrypt(new char[0]);
        SecureString testValue = keystore.getString("string_setting");
        assertThat(testValue.toString(), equalTo("stringSecretValue"));

        try (InputStream fileInput = keystore.getFile("file_setting")) {
            byte[] readBytes = new byte[20];
            assertEquals(20, fileInput.read(readBytes));
            for (int i = 0; i < fileBytes.length; ++i) {
                assertThat("byte " + i, readBytes[i], equalTo(fileBytes[i]));
            }
            assertEquals(-1, fileInput.read());
        }
    }
}
