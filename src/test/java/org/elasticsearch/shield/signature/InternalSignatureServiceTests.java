/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.signature;

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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 *
 */
public class InternalSignatureServiceTests extends ElasticsearchTestCase {

    private ResourceWatcherService watcherService;
    private Settings settings;
    private Environment env;
    private File keyFile;
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        keyFile = new File(newTempDir(), "system_key");
        Streams.copy(InternalSignatureService.generateKey(), keyFile);
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
        terminate(threadPool);
    }

    @Test
    public void testSigned() throws Exception {
        InternalSignatureService service = new InternalSignatureService(settings, env, watcherService).start();
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        assertThat(service.signed(signed), is(true));
    }

    @Test
    public void testSignAndUnsign() throws Exception {
        InternalSignatureService service = new InternalSignatureService(settings, env, watcherService).start();
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        assertThat(text.equals(signed), is(false));
        String text2 = service.unsignAndVerify(signed);
        assertThat(text, equalTo(text2));
    }

    @Test
    public void testSignAndUnsign_NoKeyFile() throws Exception {
        InternalSignatureService service = new InternalSignatureService(ImmutableSettings.EMPTY, env, watcherService).start();
        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);
        assertThat(text, equalTo(signed));
        text = service.unsignAndVerify(signed);
        assertThat(text, equalTo(signed));
    }

    @Test
    public void testReloadKey() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        InternalSignatureService service = new InternalSignatureService(settings, env, watcherService, new InternalSignatureService.Listener() {
            @Override
            public void onKeyRefresh() {
                latch.countDown();
            }
        }).start();

        String text = randomAsciiOfLength(10);
        String signed = service.sign(text);

        // we need to sleep so to ensure the timestamp of the file will definitely change
        // and so the resource watcher will pick up the change.
        sleep(1000);

        Streams.copy(InternalSignatureService.generateKey(), keyFile);
        if (!latch.await(10, TimeUnit.SECONDS)) {
            fail("waiting too long for test to complete. Expected callback is not called");
        }
        String signed2 = service.sign(text);
        assertThat(signed.equals(signed2), is(false));
    }
}
