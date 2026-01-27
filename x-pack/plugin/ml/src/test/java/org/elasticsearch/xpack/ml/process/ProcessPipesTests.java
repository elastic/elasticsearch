/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectBuilder;
import org.elasticsearch.xpack.ml.process.logging.CppLogMessageHandler;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ProcessPipesTests extends ESTestCase {

    private static final byte[] LOG_BYTES = """
        {"logger":"controller","timestamp":1478261151447,"level":"INFO","pid":42,"thread":"0x7fff7d2a8000","message":"message 5",\
        "class":"ml","method":"core::Something","file":"Something.cc","line":555}
        """.getBytes(StandardCharsets.UTF_8);
    private static final byte[] OUTPUT_BYTES = { 3 };
    private static final byte[] PERSIST_BYTES = { 6 };

    public void testProcessPipes() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);

        NamedPipeHelper namedPipeHelper = mock(NamedPipeHelper.class);
        when(namedPipeHelper.openNamedPipeInputStream(contains("log"), any(Duration.class))).thenReturn(
            new ByteArrayInputStream(LOG_BYTES)
        );
        ByteArrayOutputStream commandStream = new ByteArrayOutputStream();
        when(namedPipeHelper.openNamedPipeOutputStream(contains("command"), any(Duration.class))).thenReturn(commandStream);
        ByteArrayOutputStream processInStream = new ByteArrayOutputStream();
        when(namedPipeHelper.openNamedPipeOutputStream(contains("input"), any(Duration.class))).thenReturn(processInStream);
        when(namedPipeHelper.openNamedPipeInputStream(contains("output"), any(Duration.class))).thenReturn(
            new ByteArrayInputStream(OUTPUT_BYTES)
        );
        ByteArrayOutputStream restoreStream = new ByteArrayOutputStream();
        when(namedPipeHelper.openNamedPipeOutputStream(contains("restore"), any(Duration.class))).thenReturn(restoreStream);
        when(namedPipeHelper.openNamedPipeInputStream(contains("persist"), any(Duration.class))).thenReturn(
            new ByteArrayInputStream(PERSIST_BYTES)
        );

        int timeoutSeconds = randomIntBetween(5, 100);
        ProcessPipes processPipes = new ProcessPipes(
            env,
            namedPipeHelper,
            Duration.ofSeconds(timeoutSeconds),
            AutodetectBuilder.AUTODETECT,
            "my_job",
            null,
            false,
            true,
            true,
            true,
            true
        );

        List<String> command = new ArrayList<>();
        processPipes.addArgs(command);
        assertEquals(10, command.size());
        assertEquals(ProcessPipes.LOG_PIPE_ARG, command.get(0).substring(0, ProcessPipes.LOG_PIPE_ARG.length()));
        assertEquals(ProcessPipes.INPUT_ARG, command.get(1).substring(0, ProcessPipes.INPUT_ARG.length()));
        assertEquals(ProcessPipes.INPUT_IS_PIPE_ARG, command.get(2));
        assertEquals(ProcessPipes.OUTPUT_ARG, command.get(3).substring(0, ProcessPipes.OUTPUT_ARG.length()));
        assertEquals(ProcessPipes.OUTPUT_IS_PIPE_ARG, command.get(4));
        assertEquals(ProcessPipes.RESTORE_ARG, command.get(5).substring(0, ProcessPipes.RESTORE_ARG.length()));
        assertEquals(ProcessPipes.RESTORE_IS_PIPE_ARG, command.get(6));
        assertEquals(ProcessPipes.PERSIST_ARG, command.get(7).substring(0, ProcessPipes.PERSIST_ARG.length()));
        assertEquals(ProcessPipes.PERSIST_IS_PIPE_ARG, command.get(8));
        assertEquals(ProcessPipes.TIMEOUT_ARG + timeoutSeconds, command.get(9));

        processPipes.connectLogStream();

        CppLogMessageHandler logMessageHandler = processPipes.getLogStreamHandler();
        assertNotNull(logMessageHandler);
        logMessageHandler.tailStream();
        assertEquals(42, logMessageHandler.getPid(Duration.ZERO));

        processPipes.connectOtherStreams();

        assertFalse(processPipes.getCommandStream().isPresent());
        assertTrue(processPipes.getProcessInStream().isPresent());
        assertTrue(processPipes.getProcessOutStream().isPresent());
        assertTrue(processPipes.getRestoreStream().isPresent());
        assertTrue(processPipes.getPersistStream().isPresent());

        processPipes.getProcessInStream().get().write(2);
        byte[] processIn = processInStream.toByteArray();
        assertEquals(1, processIn.length);
        assertEquals(2, processIn[0]);
        assertEquals(3, processPipes.getProcessOutStream().get().read());
        processPipes.getRestoreStream().get().write(5);
        byte[] restoreData = restoreStream.toByteArray();
        assertEquals(1, restoreData.length);
        assertEquals(5, restoreData[0]);
        assertEquals(6, processPipes.getPersistStream().get().read());
    }

    public void testCloseUnusedPipes_notConnected() {
        NamedPipeHelper namedPipeHelper = mock(NamedPipeHelper.class);
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);

        new ProcessPipes(
            env,
            namedPipeHelper,
            Duration.ofSeconds(2),
            AutodetectBuilder.AUTODETECT,
            "my_job",
            null,
            true,
            true,
            true,
            true,
            true
        );
    }

    public void testCloseOpenedPipesOnError() throws IOException {

        NamedPipeHelper namedPipeHelper = mock(NamedPipeHelper.class);
        InputStream logStream = mock(InputStream.class);
        when(namedPipeHelper.openNamedPipeInputStream(contains("log"), any(Duration.class))).thenReturn(logStream);
        OutputStream commandStream = mock(OutputStream.class);
        when(namedPipeHelper.openNamedPipeOutputStream(contains("command"), any(Duration.class))).thenReturn(commandStream);
        OutputStream processInStream = mock(OutputStream.class);
        when(namedPipeHelper.openNamedPipeOutputStream(contains("input"), any(Duration.class))).thenReturn(processInStream);
        InputStream processOutStream = mock(InputStream.class);
        when(namedPipeHelper.openNamedPipeInputStream(contains("output"), any(Duration.class))).thenReturn(processOutStream);
        OutputStream restoreStream = mock(OutputStream.class);
        when(namedPipeHelper.openNamedPipeOutputStream(contains("restore"), any(Duration.class))).thenReturn(restoreStream);
        // opening this pipe will throw
        when(namedPipeHelper.openNamedPipeInputStream(contains("persist"), any(Duration.class))).thenThrow(new IOException());

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        ProcessPipes processPipes = new ProcessPipes(
            env,
            namedPipeHelper,
            Duration.ofSeconds(2),
            AutodetectBuilder.AUTODETECT,
            "my_job",
            null,
            true,
            true,
            true,
            true,
            true
        );

        processPipes.connectLogStream();
        expectThrows(IOException.class, processPipes::connectOtherStreams);

        // check the pipes successfully opened were then closed
        verify(logStream, times(1)).close();
        verify(commandStream, times(1)).close();
        verify(processInStream, times(1)).close();
        verify(processOutStream, times(1)).close();
        verify(restoreStream, times(1)).close();
    }
}
