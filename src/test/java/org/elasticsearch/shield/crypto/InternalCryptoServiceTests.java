/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.crypto;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class InternalCryptoServiceTests extends ElasticsearchTestCase {

    private ResourceWatcherService watcherService;
    private Settings settings;
    private Environment env;
    private File keyFile;
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        keyFile = new File(newTempDir(), "system_key");
        Streams.copy(InternalCryptoService.generateKey(), keyFile);
        settings = ImmutableSettings.builder()
                .put("shield.system_key.file", keyFile.getAbsolutePath())
                .put("watcher.interval.high", "2s")
                .build();
        env = new Environment(settings);
        threadPool = new ThreadPool("test");
        watcherService = new ResourceWatcherService(settings, threadPool);
        watcherService.start();
    }

    @After
    public void shutdown() throws InterruptedException {
        watcherService.stop();
        terminate(threadPool);
    }

    @Test
    public void testSigned() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env, watcherService).start();
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        assertThat(service.signed(signed), is(true));
    }

    @Test
    public void testSignAndUnsign() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env, watcherService).start();
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        assertThat(text.equals(signed), is(false));
        String text2 = service.unsignAndVerify(signed);
        assertThat(text, equalTo(text2));
    }

    @Test
    public void testSignAndUnsign_NoKeyFile() throws Exception {
        InternalCryptoService service = new InternalCryptoService(ImmutableSettings.EMPTY, env, watcherService).start();
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        assertThat(text, equalTo(signed));
        text = service.unsignAndVerify(signed);
        assertThat(text, equalTo(signed));
    }

    @Test
    public void testTamperedSignature() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env, watcherService).start();
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        int i = signed.indexOf("$$", 2);
        int length = Integer.parseInt(signed.substring(2, i));
        String fakeSignature = randomAsciiOfLength(length);
        String fakeSignedText = "$$" + length + "$$" + fakeSignature + signed.substring(i + 2 + length);

        try {
            service.unsignAndVerify(fakeSignedText);
        } catch (SignatureException e) {
            assertThat(e.getMessage(), is(equalTo("tampered signed text")));
            assertThat(e.getCause(), is(nullValue()));
        }
    }

    @Test
    public void testTamperedSignatureOneChar() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env, watcherService).start();
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        int i = signed.indexOf("$$", 2);
        int length = Integer.parseInt(signed.substring(2, i));
        StringBuilder fakeSignature = new StringBuilder(signed.substring(i + 2, i + 2 + length));
        fakeSignature.setCharAt(randomIntBetween(0, fakeSignature.length() - 1), randomAsciiOfLength(1).charAt(0));

        String fakeSignedText = "$$" + length + "$$" + fakeSignature.toString() + signed.substring(i + 2 + length);

        try {
            service.unsignAndVerify(fakeSignedText);
        } catch (SignatureException e) {
            assertThat(e.getMessage(), is(equalTo("tampered signed text")));
            assertThat(e.getCause(), is(nullValue()));
        }
    }

    @Test
    public void testTamperedSignatureLength() throws Exception {
        InternalCryptoService service = new InternalCryptoService(settings, env, watcherService).start();
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        int i = signed.indexOf("$$", 2);
        int length = Integer.parseInt(signed.substring(2, i));
        String fakeSignature = randomAsciiOfLength(length);

        // Smaller sig length
        String fakeSignedText = "$$" + randomIntBetween(0, length - 1) + "$$" + fakeSignature + signed.substring(i + 2 + length);

        try {
            service.unsignAndVerify(fakeSignedText);
        } catch (SignatureException e) {
            assertThat(e.getMessage(), is(equalTo("tampered signed text")));
        }

        // Larger sig length
        fakeSignedText = "$$" + randomIntBetween(length + 1, Integer.MAX_VALUE) + "$$" + fakeSignature + signed.substring(i + 2 + length);
        try {
            service.unsignAndVerify(fakeSignedText);
        } catch (SignatureException e) {
            assertThat(e.getMessage(), is(equalTo("tampered signed text")));
            assertThat(e.getCause(), is(nullValue()));
        }
    }

    @Test
    public void testEncryptionAndDecryptionChars() {
        InternalCryptoService service = new InternalCryptoService(settings, env, watcherService).start();
        final char[] chars = randomAsciiOfLengthBetween(0, 1000).toCharArray();
        final char[] encrypted = service.encrypt(chars);
        assertThat(encrypted, notNullValue());
        assertThat(Arrays.equals(encrypted, chars), is(false));

        final char[] decrypted = service.decrypt(encrypted);
        assertThat(Arrays.equals(chars, decrypted), is(true));
    }

    @Test
    public void testEncryptionAndDecryptionBytes() {
        InternalCryptoService service = new InternalCryptoService(settings, env, watcherService).start();
        final byte[] bytes = randomByteArray();
        final byte[] encrypted = service.encrypt(bytes);
        assertThat(encrypted, notNullValue());
        assertThat(Arrays.equals(encrypted, bytes), is(false));

        final byte[] decrypted = service.decrypt(encrypted);
        assertThat(Arrays.equals(bytes, decrypted), is(true));
    }

    @Test
    public void testEncryptionAndDecryptionCharsWithoutKey() {
        InternalCryptoService service = new InternalCryptoService(ImmutableSettings.EMPTY, env, watcherService).start();
        final char[] chars = randomAsciiOfLengthBetween(0, 1000).toCharArray();
        try {
            service.encrypt(chars);
            fail("exception should have been thrown");
        } catch (Exception e) {
            assertThat(e, instanceOf(UnsupportedOperationException.class));
            assertThat(e.getMessage(), containsString("system_key"));
        }

        try {
            service.decrypt(chars);
        } catch (Exception e) {
            assertThat(e, instanceOf(UnsupportedOperationException.class));
            assertThat(e.getMessage(), containsString("system_key"));
        }
    }

    @Test
    public void testEncryptionAndDecryptionBytesWithoutKey() {
        InternalCryptoService service = new InternalCryptoService(ImmutableSettings.EMPTY, env, watcherService).start();
        final byte[] bytes = randomByteArray();
        try {
            service.encrypt(bytes);
            fail("exception should have been thrown");
        } catch (Exception e) {
            assertThat(e, instanceOf(UnsupportedOperationException.class));
            assertThat(e.getMessage(), containsString("system_key"));
        }

        try {
            service.decrypt(bytes);
        } catch (Exception e) {
            assertThat(e, instanceOf(UnsupportedOperationException.class));
            assertThat(e.getMessage(), containsString("system_key"));
        }
    }

    @Test
    public void testChangingAByte() {
        InternalCryptoService service = new InternalCryptoService(settings, env, watcherService).start();
        final byte[] bytes = randomByteArray();
        final byte[] encrypted = service.encrypt(bytes);
        assertThat(encrypted, notNullValue());
        assertThat(Arrays.equals(encrypted, bytes), is(false));

        int tamperedIndex = randomIntBetween(0, encrypted.length - 1);
        encrypted[tamperedIndex] = randomByte();
        final byte[] decrypted = service.decrypt(encrypted);
        assertThat(Arrays.equals(bytes, decrypted), is(false));
    }

    @Test
    public void testReloadKey() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        InternalCryptoService service = new InternalCryptoService(settings, env, watcherService, new InternalCryptoService.Listener() {
            @Override
            public void onKeyRefresh() {
                latch.countDown();
            }
        }).start();

        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        char[] textChars = text.toCharArray();
        char[] encrypted = service.encrypt(textChars);

        // we need to sleep so to ensure the timestamp of the file will definitely change
        // and so the resource watcher will pick up the change.
        sleep(1000);

        Streams.copy(InternalCryptoService.generateKey(), keyFile);
        if (!latch.await(10, TimeUnit.SECONDS)) {
            fail("waiting too long for test to complete. Expected callback is not called");
        }
        String signed2 = service.sign(text);
        assertThat(signed.equals(signed2), is(false));

        char[] encrypted2 = service.encrypt(textChars);

        char[] decrypted = service.decrypt(encrypted);
        char[] decrypted2 = service.decrypt(encrypted2);
        assertThat(Arrays.equals(textChars, decrypted), is(false));
        assertThat(Arrays.equals(textChars, decrypted2), is(true));
    }

    private static byte[] randomByteArray() {
        int count = randomIntBetween(0, 1000);
        byte[] bytes = new byte[count];
        for (int i = 0; i < count; i++) {
            bytes[i] = randomByte();
        }
        return bytes;
    }
}
