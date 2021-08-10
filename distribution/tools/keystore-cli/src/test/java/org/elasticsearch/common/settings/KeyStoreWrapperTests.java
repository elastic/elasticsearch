/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.apache.lucene.backward_codecs.store.EndiannessReverserUtil;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

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
        final char[] password = getPossibleKeystorePassword();
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        byte[] bytes = new byte[256];
        for (int i = 0; i < 256; ++i) {
            bytes[i] = (byte) i;
        }
        keystore.setFile("foo", bytes);
        keystore.save(env.configFile(), password);
        keystore = KeyStoreWrapper.load(env.configFile());
        keystore.decrypt(password);
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

    public void testDecryptKeyStoreWithWrongPassword() throws Exception {
        final char[] realPassword = getPossibleKeystorePassword();
        final char[] invalidPassword;
        if (realPassword.length < 1) {
            invalidPassword = new char[] { 'i', 'n', 'v', 'a', 'l', 'i', 'd' };
        } else {
            invalidPassword = Arrays.copyOf(realPassword, realPassword.length + 1);
            invalidPassword[realPassword.length] = '#';
        }
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        keystore.save(env.configFile(), realPassword);
        final KeyStoreWrapper loadedkeystore = KeyStoreWrapper.load(env.configFile());
        final SecurityException exception = expectThrows(SecurityException.class, () -> loadedkeystore.decrypt(invalidPassword));
        if (inFipsJvm()) {
            assertThat(
                exception.getMessage(),
                anyOf(
                    containsString("Provided keystore password was incorrect"),
                    containsString("Keystore has been corrupted or tampered with")
                )
            );
        } else {
            assertThat(exception.getMessage(), containsString("Provided keystore password was incorrect"));
        }
    }

    public void testDecryptKeyStoreWithShortPasswordInFips() throws Exception {
        assumeTrue("This should run only in FIPS mode", inFipsJvm());
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        keystore.save(env.configFile(), "alongenoughpassword".toCharArray());
        final KeyStoreWrapper loadedkeystore = KeyStoreWrapper.load(env.configFile());
        final GeneralSecurityException exception = expectThrows(
            GeneralSecurityException.class,
            () -> loadedkeystore.decrypt("shortpwd".toCharArray()) // shorter than 14 characters
        );
        assertThat(exception.getMessage(), containsString("Error generating an encryption key from the provided password"));
    }

    public void testCreateKeyStoreWithShortPasswordInFips() throws Exception {
        assumeTrue("This should run only in FIPS mode", inFipsJvm());
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        final GeneralSecurityException exception = expectThrows(
            GeneralSecurityException.class,
            () -> keystore.save(env.configFile(), "shortpwd".toCharArray()) // shorter than 14 characters
        );
        assertThat(exception.getMessage(), containsString("Error generating an encryption key from the provided password"));
    }

    public void testCannotReadStringFromClosedKeystore() throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        assertThat(keystore.getSettingNames(), Matchers.hasItem(KeyStoreWrapper.SEED_SETTING.getKey()));
        assertThat(keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()), notNullValue());

        keystore.close();

        assertThat(keystore.getSettingNames(), Matchers.hasItem(KeyStoreWrapper.SEED_SETTING.getKey()));
        final IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey())
        );
        assertThat(exception.getMessage(), containsString("closed"));
    }

    public void testValueSHA256Digest() throws Exception {
        final KeyStoreWrapper keystore = KeyStoreWrapper.create();
        final String stringSettingKeyName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT) + "1";
        final String stringSettingValue = randomAlphaOfLength(32);
        keystore.setString(stringSettingKeyName, stringSettingValue.toCharArray());
        final String fileSettingKeyName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT) + "2";
        final byte[] fileSettingValue = randomByteArrayOfLength(32);
        keystore.setFile(fileSettingKeyName, fileSettingValue);

        final byte[] stringSettingHash = MessageDigest.getInstance("SHA-256").digest(stringSettingValue.getBytes(StandardCharsets.UTF_8));
        assertThat(keystore.getSHA256Digest(stringSettingKeyName), equalTo(stringSettingHash));
        final byte[] fileSettingHash = MessageDigest.getInstance("SHA-256").digest(fileSettingValue);
        assertThat(keystore.getSHA256Digest(fileSettingKeyName), equalTo(fileSettingHash));

        keystore.close();

        // value hashes accessible even when the keystore is closed
        assertThat(keystore.getSHA256Digest(stringSettingKeyName), equalTo(stringSettingHash));
        assertThat(keystore.getSHA256Digest(fileSettingKeyName), equalTo(fileSettingHash));
    }

    public void testUpgradeNoop() throws Exception {
        final char[] password = getPossibleKeystorePassword();
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        SecureString seed = keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey());
        keystore.save(env.configFile(), password);
        // upgrade does not overwrite seed
        KeyStoreWrapper.upgrade(keystore, env.configFile(), password);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
        keystore = KeyStoreWrapper.load(env.configFile());
        keystore.decrypt(password);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
    }

    public void testFailWhenCannotConsumeSecretStream() throws Exception {
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        Path configDir = env.configFile();
        try (
            Directory directory = newFSDirectory(configDir);
            IndexOutput indexOutput = directory.createOutput("elasticsearch.keystore", IOContext.DEFAULT)
        ) {
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
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        Path configDir = env.configFile();
        try (
            Directory directory = newFSDirectory(configDir);
            IndexOutput indexOutput = directory.createOutput("elasticsearch.keystore", IOContext.DEFAULT)
        ) {
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
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        Path configDir = env.configFile();
        try (
            Directory directory = newFSDirectory(configDir);
            IndexOutput indexOutput = directory.createOutput("elasticsearch.keystore", IOContext.DEFAULT)
        ) {
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
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        Path configDir = env.configFile();
        try (
            Directory directory = newFSDirectory(configDir);
            IndexOutput indexOutput = directory.createOutput("elasticsearch.keystore", IOContext.DEFAULT)
        ) {
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

    private void possiblyAlterEncryptedBytes(
        IndexOutput indexOutput,
        byte[] salt,
        byte[] iv,
        byte[] encryptedBytes,
        int truncEncryptedDataLength
    ) throws Exception {
        DataOutput out = EndiannessReverserUtil.wrapDataOutput(indexOutput);
        out.writeInt(4 + salt.length + 4 + iv.length + 4 + encryptedBytes.length);
        out.writeInt(salt.length);
        out.writeBytes(salt, salt.length);
        out.writeInt(iv.length);
        out.writeBytes(iv, iv.length);
        out.writeInt(encryptedBytes.length - truncEncryptedDataLength);
        out.writeBytes(encryptedBytes, encryptedBytes.length);
    }

    public void testUpgradeAddsSeed() throws Exception {
        final char[] password = getPossibleKeystorePassword();
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        keystore.remove(KeyStoreWrapper.SEED_SETTING.getKey());
        keystore.save(env.configFile(), password);
        KeyStoreWrapper.upgrade(keystore, env.configFile(), password);
        SecureString seed = keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey());
        assertNotNull(seed);
        keystore = KeyStoreWrapper.load(env.configFile());
        keystore.decrypt(password);
        assertEquals(seed.toString(), keystore.getString(KeyStoreWrapper.SEED_SETTING.getKey()).toString());
    }

    public void testIllegalSettingName() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> KeyStoreWrapper.validateSettingName("*"));
        assertTrue(e.getMessage().contains("does not match the allowed setting name pattern"));
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        e = expectThrows(IllegalArgumentException.class, () -> keystore.setString("*", new char[0]));
        assertTrue(e.getMessage().contains("does not match the allowed setting name pattern"));
        e = expectThrows(IllegalArgumentException.class, () -> keystore.setFile("*", new byte[0]));
        assertTrue(e.getMessage().contains("does not match the allowed setting name pattern"));
    }

    public void testBackcompatV1() throws Exception {
        assumeFalse("Can't run in a FIPS JVM as PBE is not available", inFipsJvm());
        Path configDir = env.configFile();
        try (
            Directory directory = newFSDirectory(configDir);
            IndexOutput output = EndiannessReverserUtil.createOutput(directory, "elasticsearch.keystore", IOContext.DEFAULT);
        ) {
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
        byte[] fileBytes = new byte[20];
        random().nextBytes(fileBytes);
        try (
            Directory directory = newFSDirectory(configDir);
            IndexOutput output = EndiannessReverserUtil.createOutput(directory, "elasticsearch.keystore", IOContext.DEFAULT);
        ) {
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
                chars[i] = (char) base64Bytes[i]; // PBE only stores the lower 8 bits, so this narrowing is ok
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

    public void testStringAndFileDistinction() throws Exception {
        final char[] password = getPossibleKeystorePassword();
        final KeyStoreWrapper wrapper = KeyStoreWrapper.create();
        wrapper.setString("string_setting", "string_value".toCharArray());
        final Path temp = createTempDir();
        Files.writeString(temp.resolve("file_setting"), "file_value", StandardCharsets.UTF_8);
        wrapper.setFile("file_setting", Files.readAllBytes(temp.resolve("file_setting")));
        wrapper.save(env.configFile(), password);
        wrapper.close();

        final KeyStoreWrapper afterSave = KeyStoreWrapper.load(env.configFile());
        assertNotNull(afterSave);
        afterSave.decrypt(password);
        assertThat(afterSave.getSettingNames(), equalTo(Set.of("keystore.seed", "string_setting", "file_setting")));
        assertThat(afterSave.getString("string_setting"), equalTo("string_value"));
        assertThat(toByteArray(afterSave.getFile("string_setting")), equalTo("string_value".getBytes(StandardCharsets.UTF_8)));
        assertThat(afterSave.getString("file_setting"), equalTo("file_value"));
        assertThat(toByteArray(afterSave.getFile("file_setting")), equalTo("file_value".getBytes(StandardCharsets.UTF_8)));
    }

    public void testLegacyV3() throws GeneralSecurityException, IOException {
        assumeFalse("Cannot open unprotected keystore on FIPS JVM", inFipsJvm());
        final Path configDir = createTempDir();
        final Path keystore = configDir.resolve("elasticsearch.keystore");
        try (
            InputStream is = KeyStoreWrapperTests.class.getResourceAsStream("/format-v3-elasticsearch.keystore");
            OutputStream os = Files.newOutputStream(keystore)
        ) {
            final byte[] buffer = new byte[4096];
            int readBytes;
            while ((readBytes = is.read(buffer)) > 0) {
                os.write(buffer, 0, readBytes);
            }
        }
        final KeyStoreWrapper wrapper = KeyStoreWrapper.load(configDir);
        assertNotNull(wrapper);
        wrapper.decrypt(new char[0]);
        assertThat(wrapper.getFormatVersion(), equalTo(3));
        assertThat(wrapper.getSettingNames(), equalTo(Set.of("keystore.seed", "string_setting", "file_setting")));
        assertThat(wrapper.getString("string_setting"), equalTo("string_value"));
        assertThat(toByteArray(wrapper.getFile("string_setting")), equalTo("string_value".getBytes(StandardCharsets.UTF_8)));
        assertThat(wrapper.getString("file_setting"), equalTo("file_value"));
        assertThat(toByteArray(wrapper.getFile("file_setting")), equalTo("file_value".getBytes(StandardCharsets.UTF_8)));
    }

    private byte[] toByteArray(final InputStream is) throws IOException {
        final ByteArrayOutputStream os = new ByteArrayOutputStream();
        final byte[] buffer = new byte[1024];
        int readBytes;
        while ((readBytes = is.read(buffer)) > 0) {
            os.write(buffer, 0, readBytes);
        }
        return os.toByteArray();
    }

    public static char[] getPossibleKeystorePassword() {
        if (inFipsJvm()) {
            // FIPS Mode JVMs require a password of at least 112 bits for the ES keystore
            return randomAlphaOfLengthBetween(14, 24).toCharArray();
        }
        return randomBoolean() ? new char[0] : randomAlphaOfLengthBetween(4, 24).toCharArray();
    }
}
