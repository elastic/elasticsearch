/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativeControllerTests extends ESTestCase {

    private static final String NODE_NAME = "native-controller-tests-node";

    private static final String TEST_MESSAGE = """
        {"logger":"controller","timestamp":1478261151445,"level":"INFO","pid":10211,"thread":"0x7fff7d2a8000",\
        "message":"controller (64 bit): Version 6.0.0-alpha1-SNAPSHOT (Build a0d6ef8819418c) Copyright (c) 2017 Elasticsearch BV",\
        "method":"main","file":"Main.cc","line":123}
        """;

    private final Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();

    public void testStartProcessCommandSucceeds() throws Exception {

        final NamedPipeHelper namedPipeHelper = mock(NamedPipeHelper.class);
        final InputStream logStream = mock(InputStream.class);
        final CountDownLatch mockNativeProcessLoggingStreamEnds = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            mockNativeProcessLoggingStreamEnds.await();
            return -1;
        }).when(logStream).read(any());
        when(namedPipeHelper.openNamedPipeInputStream(contains("log"), any(Duration.class))).thenReturn(logStream);
        ByteArrayOutputStream commandStream = new ByteArrayOutputStream();
        when(namedPipeHelper.openNamedPipeOutputStream(contains("command"), any(Duration.class))).thenReturn(commandStream);
        ByteArrayInputStream outputStream = new ByteArrayInputStream("""
            [{"id":1,"success":true,"reason":"ok"}]""".getBytes(StandardCharsets.UTF_8));
        when(namedPipeHelper.openNamedPipeInputStream(contains("output"), any(Duration.class))).thenReturn(outputStream);

        List<String> command = new ArrayList<>();
        command.add("my_process");
        command.add("--arg1");
        command.add("--arg2=42");
        command.add("--arg3=something with spaces");

        NativeController nativeController = new NativeController(
            NODE_NAME,
            TestEnvironment.newEnvironment(settings),
            namedPipeHelper,
            mock(NamedXContentRegistry.class)
        );
        nativeController.startProcess(command);

        assertEquals(
            "1\tstart\tmy_process\t--arg1\t--arg2=42\t--arg3=something with spaces\n",
            commandStream.toString(StandardCharsets.UTF_8)
        );

        mockNativeProcessLoggingStreamEnds.countDown();
    }

    public void testStartProcessCommandFails() throws Exception {

        final NamedPipeHelper namedPipeHelper = mock(NamedPipeHelper.class);
        final InputStream logStream = mock(InputStream.class);
        final CountDownLatch mockNativeProcessLoggingStreamEnds = new CountDownLatch(1);
        doAnswer(invocationOnMock -> {
            mockNativeProcessLoggingStreamEnds.await();
            return -1;
        }).when(logStream).read(any());
        when(namedPipeHelper.openNamedPipeInputStream(contains("log"), any(Duration.class))).thenReturn(logStream);
        ByteArrayOutputStream commandStream = new ByteArrayOutputStream();
        when(namedPipeHelper.openNamedPipeOutputStream(contains("command"), any(Duration.class))).thenReturn(commandStream);
        ByteArrayInputStream outputStream = new ByteArrayInputStream("""
            [{"id":1,"success":false,"reason":"some problem"}]""".getBytes(StandardCharsets.UTF_8));
        when(namedPipeHelper.openNamedPipeInputStream(contains("output"), any(Duration.class))).thenReturn(outputStream);

        List<String> command = new ArrayList<>();
        command.add("my_process");
        command.add("--arg1");
        command.add("--arg2=666");
        command.add("--arg3=something different with spaces");

        NativeController nativeController = new NativeController(
            NODE_NAME,
            TestEnvironment.newEnvironment(settings),
            namedPipeHelper,
            mock(NamedXContentRegistry.class)
        );
        IOException e = expectThrows(IOException.class, () -> nativeController.startProcess(command));

        assertEquals(
            "1\tstart\tmy_process\t--arg1\t--arg2=666\t--arg3=something different with spaces\n",
            commandStream.toString(StandardCharsets.UTF_8)
        );
        assertEquals("ML controller failed to execute command [1]: [some problem]", e.getMessage());

        mockNativeProcessLoggingStreamEnds.countDown();
    }

    public void testGetNativeCodeInfo() throws IOException, TimeoutException {

        NamedPipeHelper namedPipeHelper = mock(NamedPipeHelper.class);
        ByteArrayInputStream logStream = new ByteArrayInputStream(TEST_MESSAGE.getBytes(StandardCharsets.UTF_8));
        when(namedPipeHelper.openNamedPipeInputStream(contains("log"), any(Duration.class))).thenReturn(logStream);
        ByteArrayOutputStream commandStream = new ByteArrayOutputStream();
        when(namedPipeHelper.openNamedPipeOutputStream(contains("command"), any(Duration.class))).thenReturn(commandStream);
        ByteArrayInputStream outputStream = new ByteArrayInputStream("[]".getBytes(StandardCharsets.UTF_8));
        when(namedPipeHelper.openNamedPipeInputStream(contains("output"), any(Duration.class))).thenReturn(outputStream);

        NativeController nativeController = new NativeController(
            NODE_NAME,
            TestEnvironment.newEnvironment(settings),
            namedPipeHelper,
            mock(NamedXContentRegistry.class)
        );
        Map<String, Object> nativeCodeInfo = nativeController.getNativeCodeInfo();

        assertNotNull(nativeCodeInfo);
        assertEquals(2, nativeCodeInfo.size());
        assertEquals("6.0.0-alpha1-SNAPSHOT", nativeCodeInfo.get("version"));
        assertEquals("a0d6ef8819418c", nativeCodeInfo.get("build_hash"));
    }

    public void testControllerDeath() throws Exception {

        NamedPipeHelper namedPipeHelper = mock(NamedPipeHelper.class);
        ByteArrayInputStream logStream = new ByteArrayInputStream(TEST_MESSAGE.getBytes(StandardCharsets.UTF_8));
        when(namedPipeHelper.openNamedPipeInputStream(contains("log"), any(Duration.class))).thenReturn(logStream);
        ByteArrayOutputStream commandStream = new ByteArrayOutputStream();
        when(namedPipeHelper.openNamedPipeOutputStream(contains("command"), any(Duration.class))).thenReturn(commandStream);
        ByteArrayInputStream outputStream = new ByteArrayInputStream("[".getBytes(StandardCharsets.UTF_8));
        when(namedPipeHelper.openNamedPipeInputStream(contains("output"), any(Duration.class))).thenReturn(outputStream);

        NativeController nativeController = new NativeController(
            NODE_NAME,
            TestEnvironment.newEnvironment(settings),
            namedPipeHelper,
            mock(NamedXContentRegistry.class)
        );

        // As soon as the log stream ends startProcess should think the native controller has died
        assertBusy(() -> {
            ElasticsearchException e = expectThrows(
                ElasticsearchException.class,
                () -> nativeController.startProcess(Collections.singletonList("my process"))
            );

            assertEquals(
                "Cannot start process [my process]: native controller process has stopped on node " + "[native-controller-tests-node]",
                e.getMessage()
            );
        });
    }
}
