/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativeControllerTests extends ESTestCase {

    private static final String NODE_NAME = "native-controller-tests-node";

    private static final String TEST_MESSAGE = "{\"logger\":\"controller\",\"timestamp\":1478261151445,\"level\":\"INFO\",\"pid\":10211,"
            + "\"thread\":\"0x7fff7d2a8000\",\"message\":\"controller (64 bit): Version 6.0.0-alpha1-SNAPSHOT (Build a0d6ef8819418c) "
            + "Copyright (c) 2017 Elasticsearch BV\",\"method\":\"main\",\"file\":\"Main.cc\",\"line\":123}\n";

    private Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();

    public void testStartProcessCommand() throws IOException {

        final NamedPipeHelper namedPipeHelper = mock(NamedPipeHelper.class);
        final InputStream logStream = mock(InputStream.class);
        final CountDownLatch mockNativeProcessLoggingStreamEnds = new CountDownLatch(1);
        doAnswer(
            invocationOnMock -> {
                mockNativeProcessLoggingStreamEnds.await();
                return -1;
            }).when(logStream).read(any());
        when(namedPipeHelper.openNamedPipeInputStream(contains("log"), any(Duration.class))).thenReturn(logStream);
        ByteArrayOutputStream commandStream = new ByteArrayOutputStream();
        when(namedPipeHelper.openNamedPipeOutputStream(contains("command"), any(Duration.class))).thenReturn(commandStream);

        List<String> command = new ArrayList<>();
        command.add("my_process");
        command.add("--arg1");
        command.add("--arg2=42");
        command.add("--arg3=something with spaces");

        NativeController nativeController = new NativeController(NODE_NAME, TestEnvironment.newEnvironment(settings), namedPipeHelper);
        nativeController.startProcess(command);

        assertEquals("start\tmy_process\t--arg1\t--arg2=42\t--arg3=something with spaces\n",
                commandStream.toString(StandardCharsets.UTF_8.name()));

        mockNativeProcessLoggingStreamEnds.countDown();
    }

    public void testGetNativeCodeInfo() throws IOException, TimeoutException {

        NamedPipeHelper namedPipeHelper = mock(NamedPipeHelper.class);
        ByteArrayInputStream logStream = new ByteArrayInputStream(TEST_MESSAGE.getBytes(StandardCharsets.UTF_8));
        when(namedPipeHelper.openNamedPipeInputStream(contains("log"), any(Duration.class))).thenReturn(logStream);
        ByteArrayOutputStream commandStream = new ByteArrayOutputStream();
        when(namedPipeHelper.openNamedPipeOutputStream(contains("command"), any(Duration.class))).thenReturn(commandStream);

        NativeController nativeController = new NativeController(NODE_NAME, TestEnvironment.newEnvironment(settings), namedPipeHelper);
        Map<String, Object> nativeCodeInfo = nativeController.getNativeCodeInfo();

        assertNotNull(nativeCodeInfo);
        assertEquals(2, nativeCodeInfo.size());
        assertEquals("6.0.0-alpha1-SNAPSHOT", nativeCodeInfo.get("version"));
        assertEquals("a0d6ef8819418c", nativeCodeInfo.get("build_hash"));
    }

    public void testControllerDeath() throws Exception {

        NamedPipeHelper namedPipeHelper =  mock(NamedPipeHelper.class);
        ByteArrayInputStream logStream = new ByteArrayInputStream(TEST_MESSAGE.getBytes(StandardCharsets.UTF_8));
        when(namedPipeHelper.openNamedPipeInputStream(contains("log"), any(Duration.class))).thenReturn(logStream);
        ByteArrayOutputStream commandStream = new ByteArrayOutputStream();
        when(namedPipeHelper.openNamedPipeOutputStream(contains("command"), any(Duration.class))).thenReturn(commandStream);

        NativeController nativeController = new NativeController(NODE_NAME, TestEnvironment.newEnvironment(settings), namedPipeHelper);

        // As soon as the log stream ends startProcess should think the native controller has died
        assertBusy(() -> {
            ElasticsearchException e = expectThrows(ElasticsearchException.class,
                    () -> nativeController.startProcess(Collections.singletonList("my process")));

            assertEquals("Cannot start process [my process]: native controller process has stopped on node " +
                "[native-controller-tests-node]", e.getMessage());
        });
    }
}
