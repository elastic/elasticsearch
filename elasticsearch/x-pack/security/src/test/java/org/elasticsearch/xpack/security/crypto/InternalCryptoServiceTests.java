/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.crypto;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class InternalCryptoServiceTests extends ESTestCase {
    private Settings settings;
    private Environment env;
    private Path keyFile;

    @Before
    public void init() throws Exception {
        keyFile = createTempDir().resolve("system_key");
        Files.write(keyFile, InternalCryptoService.generateKey());
        settings = Settings.builder()
                .put(InternalCryptoService.FILE_SETTING.getKey(), keyFile.toAbsolutePath())
                .put("resource.reload.interval.high", "2s")
                .put("path.home", createTempDir())
                .build();
        env = new Environment(settings);
    }

    public void testSigned() throws Exception {
        // randomize whether to use a system key or not
        Settings settings = randomBoolean() ? this.settings : Settings.EMPTY;
        InternalCryptoService service = new InternalCryptoService(settings, env);
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        assertThat(service.isSigned(signed), is(true));
    }

    public void testSignAndUnsign() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env);
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        assertThat(text.equals(signed), is(false));
        String text2 = service.unsignAndVerify(signed);
        assertThat(text, equalTo(text2));
    }

    public void testSignAndUnsignNoKeyFile() throws Exception {
        InternalCryptoService service = new InternalCryptoService(Settings.EMPTY, env);
        final String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        // we always have some sort of key to sign with
        assertThat(text, not(equalTo(signed)));
        String unsigned = service.unsignAndVerify(signed);
        assertThat(unsigned, equalTo(text));
    }

    public void testTamperedSignature() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env);
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        int i = signed.indexOf("$$", 2);
        int length = Integer.parseInt(signed.substring(2, i));
        String fakeSignature = randomAsciiOfLength(length);
        String fakeSignedText = "$$" + length + "$$" + fakeSignature + signed.substring(i + 2 + length);

        try {
            service.unsignAndVerify(fakeSignedText);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(equalTo("tampered signed text")));
            assertThat(e.getCause(), is(nullValue()));
        }
    }

    public void testTamperedSignatureOneChar() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env);
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        int i = signed.indexOf("$$", 2);
        int length = Integer.parseInt(signed.substring(2, i));
        StringBuilder fakeSignature = new StringBuilder(signed.substring(i + 2, i + 2 + length));
        fakeSignature.setCharAt(randomIntBetween(0, fakeSignature.length() - 1), randomAsciiOfLength(1).charAt(0));

        String fakeSignedText = "$$" + length + "$$" + fakeSignature.toString() + signed.substring(i + 2 + length);

        try {
            service.unsignAndVerify(fakeSignedText);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(equalTo("tampered signed text")));
            assertThat(e.getCause(), is(nullValue()));
        }
    }

    public void testTamperedSignatureLength() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env);
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        int i = signed.indexOf("$$", 2);
        int length = Integer.parseInt(signed.substring(2, i));
        String fakeSignature = randomAsciiOfLength(length);

        // Smaller sig length
        String fakeSignedText = "$$" + randomIntBetween(0, length - 1) + "$$" + fakeSignature + signed.substring(i + 2 + length);

        try {
            service.unsignAndVerify(fakeSignedText);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(equalTo("tampered signed text")));
        }

        // Larger sig length
        fakeSignedText = "$$" + randomIntBetween(length + 1, Integer.MAX_VALUE) + "$$" + fakeSignature + signed.substring(i + 2 + length);
        try {
            service.unsignAndVerify(fakeSignedText);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is(equalTo("tampered signed text")));
            assertThat(e.getCause(), is(nullValue()));
        }
    }

    public void testEncryptionAndDecryptionChars() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env);
                assertThat(service.isEncryptionEnabled(), is(true));
        final char[] chars = randomAsciiOfLengthBetween(0, 1000).toCharArray();
        final char[] encrypted = service.encrypt(chars);
        assertThat(encrypted, notNullValue());
        assertThat(Arrays.equals(encrypted, chars), is(false));

        final char[] decrypted = service.decrypt(encrypted);
        assertThat(Arrays.equals(chars, decrypted), is(true));
    }

    public void testEncryptionAndDecryptionCharsWithoutKey() throws Exception {
        InternalCryptoService service = new InternalCryptoService(Settings.EMPTY, env);
                assertThat(service.isEncryptionEnabled(), is(false));
        final char[] chars = randomAsciiOfLengthBetween(0, 1000).toCharArray();
        final char[] encryptedChars = service.encrypt(chars);
        final char[] decryptedChars = service.decrypt(encryptedChars);
        assertThat(chars, equalTo(encryptedChars));
        assertThat(chars, equalTo(decryptedChars));
    }

    public void testEncryptionEnabledWithKey() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env);
                assertThat(service.isEncryptionEnabled(), is(true));
    }

    public void testEncryptionEnabledWithoutKey() throws Exception {
        InternalCryptoService service = new InternalCryptoService(Settings.EMPTY, env);
                assertThat(service.isEncryptionEnabled(), is(false));
    }

    public void testEncryptedChar() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env);
                assertThat(service.isEncryptionEnabled(), is(true));

        assertThat(service.isEncrypted((char[]) null), is(false));
        assertThat(service.isEncrypted(new char[0]), is(false));
        assertThat(service.isEncrypted(new char[InternalCryptoService.ENCRYPTED_TEXT_PREFIX.length()]), is(false));
        assertThat(service.isEncrypted(InternalCryptoService.ENCRYPTED_TEXT_PREFIX.toCharArray()), is(true));
        assertThat(service.isEncrypted(randomAsciiOfLengthBetween(0, 100).toCharArray()), is(false));
        assertThat(service.isEncrypted(service.encrypt(randomAsciiOfLength(10).toCharArray())), is(true));
    }

    public void testSigningKeyCanBeRecomputedConsistently() {
        final SecretKey systemKey = new SecretKeySpec(InternalCryptoService.generateKey(), InternalCryptoService.KEY_ALGO);
        final SecretKey randomKey = InternalCryptoService.generateSecretKey(InternalCryptoService.RANDOM_KEY_SIZE);
        int iterations = randomInt(100);
        final SecretKey signingKey = InternalCryptoService.createSigningKey(systemKey, randomKey);
        for (int i = 0; i < iterations; i++) {
            SecretKey regenerated = InternalCryptoService.createSigningKey(systemKey, randomKey);
            assertThat(regenerated, equalTo(signingKey));
        }
    }
}
