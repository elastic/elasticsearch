/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.KeyStore;
import java.util.Base64;

public class FIPS140SecureSettingsBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    public void testLegacySecureSettingsIsNotAllowed() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, PBE is not available", inFipsJvm());
        final Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.fips_mode.enabled", "true");
        Environment env = TestEnvironment.newEnvironment(builder.build());
        generateV2Keystore(env);
        assertTrue(new FIPS140SecureSettingsBootstrapCheck(builder.build(), env).check(createTestContext(builder.build(),
            null)).isFailure());
    }

    public void testCorrectSecureSettingsVersionIsAllowed() throws Exception {
        final Settings.Builder builder = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.fips_mode.enabled", "true");
        Environment env = TestEnvironment.newEnvironment(builder.build());
        final KeyStoreWrapper keyStoreWrapper = KeyStoreWrapper.create();
        try {
            keyStoreWrapper.save(env.configFile(), "password".toCharArray());
        } catch (final AccessControlException e) {
            if (e.getPermission() instanceof RuntimePermission && e.getPermission().getName().equals("accessUserInformation")) {
                // this is expected:but we don't care in tests
            } else {
                throw e;
            }
        }
        assertFalse(new FIPS140SecureSettingsBootstrapCheck(builder.build(), env).check(createTestContext(builder.build(),
            null)).isFailure());
    }

    private void generateV2Keystore(Environment env) throws Exception {
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
    }
}
