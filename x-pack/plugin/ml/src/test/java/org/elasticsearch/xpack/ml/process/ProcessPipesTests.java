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
import org.elasticsearch.monitor.jvm.JvmInfo;
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
            true,
            false
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
            true,
            false
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
            true,
            false
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

    public void testPipeNaming_isolationOff_pinnedLegacyNames() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        NamedPipeHelper namedPipeHelper = new NamedPipeHelper();

        ProcessPipes processPipes = new ProcessPipes(
            env,
            namedPipeHelper,
            Duration.ofSeconds(10),
            "myproc",
            "my_job",
            42L,
            true,
            true,
            true,
            true,
            true,
            false
        );

        List<String> command = new ArrayList<>();
        processPipes.addArgs(command);

        // Pin the exact legacy naming scheme: <defaultPipeDirPrefix><processName>_<jobId>_<uniqueId>_<pipe><_pid>
        String prefix = namedPipeHelper.getDefaultPipeDirectoryPrefix(env) + "myproc_my_job_42_";
        String suffix = "_" + JvmInfo.jvmInfo().getPid();
        assertEquals(11, command.size());
        assertEquals(ProcessPipes.LOG_PIPE_ARG + prefix + "log" + suffix, command.get(0));
        assertEquals(ProcessPipes.COMMAND_PIPE_ARG + prefix + "command" + suffix, command.get(1));
        assertEquals(ProcessPipes.INPUT_ARG + prefix + "input" + suffix, command.get(2));
        assertEquals(ProcessPipes.INPUT_IS_PIPE_ARG, command.get(3));
        assertEquals(ProcessPipes.OUTPUT_ARG + prefix + "output" + suffix, command.get(4));
        assertEquals(ProcessPipes.OUTPUT_IS_PIPE_ARG, command.get(5));
        assertEquals(ProcessPipes.RESTORE_ARG + prefix + "restore" + suffix, command.get(6));
        assertEquals(ProcessPipes.RESTORE_IS_PIPE_ARG, command.get(7));
        assertEquals(ProcessPipes.PERSIST_ARG + prefix + "persist" + suffix, command.get(8));
        assertEquals(ProcessPipes.PERSIST_IS_PIPE_ARG, command.get(9));
        assertEquals(ProcessPipes.TIMEOUT_ARG + 10, command.get(10));
    }

    public void testPipeNaming_isolationOnLinux_usesChildIpcDirectory() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        NamedPipeHelper namedPipeHelper = new NamedPipeHelper();

        ProcessPipes processPipes = new ProcessPipes(
            env,
            namedPipeHelper,
            Duration.ofSeconds(10),
            "myproc",
            "deployment-1",
            null,
            false,
            true,
            true,
            true,
            false,
            true,
            true // isLinux
        );

        List<String> command = new ArrayList<>();
        processPipes.addArgs(command);

        String childIpcDir = namedPipeHelper.getChildIpcDirectoryPrefix(env, "deployment-1");
        assertEquals(8, command.size());
        assertEquals(ProcessPipes.LOG_PIPE_ARG + childIpcDir + "logPipe", command.get(0));
        assertEquals(ProcessPipes.INPUT_ARG + childIpcDir + "input", command.get(1));
        assertEquals(ProcessPipes.INPUT_IS_PIPE_ARG, command.get(2));
        assertEquals(ProcessPipes.OUTPUT_ARG + childIpcDir + "output", command.get(3));
        assertEquals(ProcessPipes.OUTPUT_IS_PIPE_ARG, command.get(4));
        assertEquals(ProcessPipes.RESTORE_ARG + childIpcDir + "restore", command.get(5));
        assertEquals(ProcessPipes.RESTORE_IS_PIPE_ARG, command.get(6));
        assertEquals(ProcessPipes.TIMEOUT_ARG + 10, command.get(7));
    }

    public void testChildIpcDirectory_disjointPerJobId() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        NamedPipeHelper namedPipeHelper = new NamedPipeHelper();

        ProcessPipes pipesA = new ProcessPipes(
            env,
            namedPipeHelper,
            Duration.ofSeconds(10),
            "myproc",
            "job-a",
            null,
            false,
            true,
            false,
            false,
            false,
            true,
            true
        );
        ProcessPipes pipesB = new ProcessPipes(
            env,
            namedPipeHelper,
            Duration.ofSeconds(10),
            "myproc",
            "job-b",
            null,
            false,
            true,
            false,
            false,
            false,
            true,
            true
        );

        List<String> commandA = new ArrayList<>();
        pipesA.addArgs(commandA);
        List<String> commandB = new ArrayList<>();
        pipesB.addArgs(commandB);

        String inputA = commandA.get(1).substring(ProcessPipes.INPUT_ARG.length());
        String inputB = commandB.get(1).substring(ProcessPipes.INPUT_ARG.length());

        assertNotEquals(inputA, inputB);
        assertTrue(inputA.contains("job-a"));
        assertTrue(inputB.contains("job-b"));
        assertFalse(inputA.contains("job-b"));
        assertFalse(inputB.contains("job-a"));
    }

    public void testPipeNaming_isolationRequestedButNotLinux_fallsBackToLegacyNaming() {
        // Constants.LINUX is a static final boolean and cannot be overridden in-process (see PyTorchBuilderTests
        // for the same limitation with PyTorchBuilder). This test uses the package-private constructor to inject
        // isLinux=false directly, which is the only way to exercise the "isolation requested but not Linux" branch;
        // it does not prove Constants.LINUX itself evaluates correctly on a real non-Linux host.
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        NamedPipeHelper namedPipeHelper = new NamedPipeHelper();

        ProcessPipes processPipes = new ProcessPipes(
            env,
            namedPipeHelper,
            Duration.ofSeconds(10),
            "myproc",
            "my_job",
            42L,
            true,
            true,
            true,
            true,
            true,
            true, // useIsolatedChildIpcDir requested
            false // isLinux
        );

        List<String> command = new ArrayList<>();
        processPipes.addArgs(command);

        String prefix = namedPipeHelper.getDefaultPipeDirectoryPrefix(env) + "myproc_my_job_42_";
        String suffix = "_" + JvmInfo.jvmInfo().getPid();
        assertEquals(ProcessPipes.LOG_PIPE_ARG + prefix + "log" + suffix, command.get(0));
    }

    public void testPipeNaming_isolationRequestedButNoJobId_fallsBackToLegacyNaming() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        NamedPipeHelper namedPipeHelper = new NamedPipeHelper();

        ProcessPipes processPipes = new ProcessPipes(
            env,
            namedPipeHelper,
            Duration.ofSeconds(10),
            "myproc",
            null,
            null,
            false,
            true,
            false,
            false,
            false,
            true,
            true
        );

        List<String> command = new ArrayList<>();
        processPipes.addArgs(command);

        String prefix = namedPipeHelper.getDefaultPipeDirectoryPrefix(env) + "myproc_";
        String suffix = "_" + JvmInfo.jvmInfo().getPid();
        assertEquals(ProcessPipes.LOG_PIPE_ARG + prefix + "log" + suffix, command.get(0));
    }

    public void testIsolatedChildIpcDir_rejectsPersistPipe() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        NamedPipeHelper namedPipeHelper = new NamedPipeHelper();

        expectThrows(
            IllegalArgumentException.class,
            () -> new ProcessPipes(
                env,
                namedPipeHelper,
                Duration.ofSeconds(10),
                "myproc",
                "my_job",
                null,
                false,
                true,
                true,
                true,
                true, // wantPersistPipe
                true,
                true
            )
        );
    }

    public void testIsolatedChildIpcDir_rejectsCommandPipe() {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build();
        Environment env = TestEnvironment.newEnvironment(settings);
        NamedPipeHelper namedPipeHelper = new NamedPipeHelper();

        expectThrows(
            IllegalArgumentException.class,
            () -> new ProcessPipes(
                env,
                namedPipeHelper,
                Duration.ofSeconds(10),
                "myproc",
                "my_job",
                null,
                true, // wantCommandPipe
                true,
                true,
                true,
                false,
                true,
                true
            )
        );
    }
}
