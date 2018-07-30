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

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.instanceOf;

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

    public void testCannotReadStringFromClosedKeystore() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        assertThat(keystore.getSettingNames(), Matchers.hasItem(KeyStoreWrapper.SEED_SETTING.getKey()));
        assertThat(keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()), notNullValue());

        keystore.close();

        assertThat(keystore.getSettingNames(), Matchers.hasItem(KeyStoreWrapper.SEED_SETTING.getKey()));
        final IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()));
        assertThat(exception.getMessage(), containsString("closed"));
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

    public void testFailWhenCannotConsumeSecretStream() throws Exception {
        Path configDir = env.configFile();
        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        try (IndexOutput indexOutput = directory.createOutput("elasticsearch.keystore", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(indexOutput, "elasticsearch.keystore", 3);
            indexOutput.writeByte((byte) 0); // No password
            SecureRandom random = Randomness.createSecure();
            byte[] salt = new byte[64];
            random.nextBytes(salt);
            byte[] iv = new byte[12];
            random.nextBytes(iv);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CipherOutputStream cipherStream = getCipherStream(bytes, salt, iv);
            DataOutputStream output = new DataOutputStream(cipherStream);
            // Indicate that the secret string is longer than it is so readFully() fails
            possiblyAlterSecretString(output, -4);
            cipherStream.close();
            final byte[] encryptedBytes = bytes.toByteArray();
            possiblyAlterEncryptedBytes(indexOutput, salt, iv, encryptedBytes, 0);
            CodecUtil.writeFooter(indexOutput);
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        SecurityException e = expectThrows(SecurityException.class, () -> keystore.decrypt(new char[0]));
        assertThat(e.getMessage(), containsString("Keystore has been corrupted or tampered with"));
        assertThat(e.getCause(), instanceOf(EOFException.class));
    }

    public void testFailWhenCannotConsumeEncryptedBytesStream() throws Exception {
        Path configDir = env.configFile();
        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        try (IndexOutput indexOutput = directory.createOutput("elasticsearch.keystore", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(indexOutput, "elasticsearch.keystore", 3);
            indexOutput.writeByte((byte) 0); // No password
            SecureRandom random = Randomness.createSecure();
            byte[] salt = new byte[64];
            random.nextBytes(salt);
            byte[] iv = new byte[12];
            random.nextBytes(iv);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CipherOutputStream cipherStream = getCipherStream(bytes, salt, iv);
            DataOutputStream output = new DataOutputStream(cipherStream);

            possiblyAlterSecretString(output, 0);
            cipherStream.close();
            final byte[] encryptedBytes = bytes.toByteArray();
            // Indicate that the encryptedBytes is larger than it is so readFully() fails
            possiblyAlterEncryptedBytes(indexOutput, salt, iv, encryptedBytes, -12);
            CodecUtil.writeFooter(indexOutput);
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        SecurityException e = expectThrows(SecurityException.class, () -> keystore.decrypt(new char[0]));
        assertThat(e.getMessage(), containsString("Keystore has been corrupted or tampered with"));
        assertThat(e.getCause(), instanceOf(EOFException.class));
    }

    public void testFailWhenSecretStreamNotConsumed() throws Exception {
        Path configDir = env.configFile();
        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        try (IndexOutput indexOutput = directory.createOutput("elasticsearch.keystore", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(indexOutput, "elasticsearch.keystore", 3);
            indexOutput.writeByte((byte) 0); // No password
            SecureRandom random = Randomness.createSecure();
            byte[] salt = new byte[64];
            random.nextBytes(salt);
            byte[] iv = new byte[12];
            random.nextBytes(iv);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CipherOutputStream cipherStream = getCipherStream(bytes, salt, iv);
            DataOutputStream output = new DataOutputStream(cipherStream);
            // So that readFully during decryption will not consume the entire stream
            possiblyAlterSecretString(output, 4);
            cipherStream.close();
            final byte[] encryptedBytes = bytes.toByteArray();
            possiblyAlterEncryptedBytes(indexOutput, salt, iv, encryptedBytes, 0);
            CodecUtil.writeFooter(indexOutput);
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        SecurityException e = expectThrows(SecurityException.class, () -> keystore.decrypt(new char[0]));
        assertThat(e.getMessage(), containsString("Keystore has been corrupted or tampered with"));
    }

    public void testFailWhenEncryptedBytesStreamIsNotConsumed() throws Exception {
        Path configDir = env.configFile();
        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        try (IndexOutput indexOutput = directory.createOutput("elasticsearch.keystore", IOContext.DEFAULT)) {
            CodecUtil.writeHeader(indexOutput, "elasticsearch.keystore", 3);
            indexOutput.writeByte((byte) 0); // No password
            SecureRandom random = Randomness.createSecure();
            byte[] salt = new byte[64];
            random.nextBytes(salt);
            byte[] iv = new byte[12];
            random.nextBytes(iv);
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            CipherOutputStream cipherStream = getCipherStream(bytes, salt, iv);
            DataOutputStream output = new DataOutputStream(cipherStream);
            possiblyAlterSecretString(output, 0);
            cipherStream.close();
            final byte[] encryptedBytes = bytes.toByteArray();
            possiblyAlterEncryptedBytes(indexOutput, salt, iv, encryptedBytes, randomIntBetween(2, encryptedBytes.length));
            CodecUtil.writeFooter(indexOutput);
        }

        KeyStoreWrapper keystore = KeyStoreWrapper.load(configDir);
        SecurityException e = expectThrows(SecurityException.class, () -> keystore.decrypt(new char[0]));
        assertThat(e.getMessage(), containsString("Keystore has been corrupted or tampered with"));
    }

    private CipherOutputStream getCipherStream(ByteArrayOutputStream bytes, byte[] salt, byte[] iv) throws Exception {
        PBEKeySpec keySpec = new PBEKeySpec(new char[0], salt, 10000, 128);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
        SecretKey secretKey = keyFactory.generateSecret(keySpec);
        SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), "AES");
        GCMParameterSpec spec = new GCMParameterSpec(128, iv);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, secret, spec);
        cipher.updateAAD(salt);
        return new CipherOutputStream(bytes, cipher);
    }

    private void possiblyAlterSecretString(DataOutputStream output, int truncLength) throws Exception {
        byte[] secret_value = "super_secret_value".getBytes(StandardCharsets.UTF_8);
        output.writeInt(1); // One entry
        output.writeUTF("string_setting");
        output.writeUTF("STRING");
        output.writeInt(secret_value.length - truncLength);
        output.write(secret_value);
    }

    private void possiblyAlterEncryptedBytes(IndexOutput indexOutput, byte[] salt, byte[] iv, byte[] encryptedBytes, int
        truncEncryptedDataLength)
        throws Exception {
        indexOutput.writeInt(4 + salt.length + 4 + iv.length + 4 + encryptedBytes.length);
        indexOutput.writeInt(salt.length);
        indexOutput.writeBytes(salt, salt.length);
        indexOutput.writeInt(iv.length);
        indexOutput.writeBytes(iv, iv.length);
        indexOutput.writeInt(encryptedBytes.length - truncEncryptedDataLength);
        indexOutput.writeBytes(encryptedBytes, encryptedBytes.length);
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
        assumeFalse("Can't run in a FIPS JVM as PBE is not available", inFipsJvm());
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
        assumeFalse("Can't run in a FIPS JVM as PBE is not available", inFipsJvm());
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
