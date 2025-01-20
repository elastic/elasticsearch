/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.common.Strings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xpack.ml.process.logging.CppLogMessageHandler;
import org.elasticsearch.xpack.ml.utils.FileUtils;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * Utility class for telling a Ml C++ process which named pipes to use,
 * and then waiting for them to connect once the C++ process is running.
 */
public class ProcessPipes {

    public static final String LOG_PIPE_ARG = "--logPipe=";
    public static final String COMMAND_PIPE_ARG = "--commandPipe=";
    public static final String INPUT_ARG = "--input=";
    public static final String INPUT_IS_PIPE_ARG = "--inputIsPipe";
    public static final String OUTPUT_ARG = "--output=";
    public static final String OUTPUT_IS_PIPE_ARG = "--outputIsPipe";
    public static final String RESTORE_ARG = "--restore=";
    public static final String RESTORE_IS_PIPE_ARG = "--restoreIsPipe";
    public static final String PERSIST_ARG = "--persist=";
    public static final String PERSIST_IS_PIPE_ARG = "--persistIsPipe";
    public static final String TIMEOUT_ARG = "--namedPipeConnectTimeout=";

    private final NamedPipeHelper namedPipeHelper;
    private final String jobId;
    private final Path tempDir;

    /**
     * <code>null</code> indicates a pipe won't be used
     */
    private final String logPipeName;
    private final String commandPipeName;
    private final String processInPipeName;
    private final String processOutPipeName;
    private final String restorePipeName;
    private final String persistPipeName;

    /**
     * Needs to be long enough for the C++ process perform all startup tasks that precede creation of named
     * pipes.  There should not be very many of these, so a short timeout should be fine.  However, at least
     * five seconds is recommended due to the vagaries of process scheduling and the way VMs can completely
     * stall for some hypervisor actions.
     */
    private final Duration timeout;

    private CppLogMessageHandler logStreamHandler;
    private OutputStream commandStream;
    private OutputStream processInStream;
    private InputStream processOutStream;
    private OutputStream restoreStream;
    private InputStream persistStream;

    /**
     * Construct, stating which pipes are expected to be created.  The corresponding C++ process creates the named pipes, so
     * <em>only one combination of wanted pipes will work with any given C++ process</em>.  The arguments to this constructor
     * must be carefully chosen with reference to the corresponding C++ code.
     * @param processName The name of the process that pipes are to be opened to.
     *                    Must not be a full path, nor have the .exe extension on Windows.
     * @param jobId The job ID of the process to which pipes are to be opened, if the process is associated with a specific job.
     *              May be null or empty for processes not associated with a specific job.
     */
    public ProcessPipes(
        Environment env,
        NamedPipeHelper namedPipeHelper,
        Duration timeout,
        String processName,
        String jobId,
        Long uniqueId,
        boolean wantCommandPipe,
        boolean wantProcessInPipe,
        boolean wantProcessOutPipe,
        boolean wantRestorePipe,
        boolean wantPersistPipe
    ) {
        this.namedPipeHelper = namedPipeHelper;
        this.jobId = jobId;
        this.tempDir = env.tmpFile();
        this.timeout = timeout;

        // The way the pipe names are formed MUST match what is done in the controller main()
        // function, as it does not get any command line arguments when started as a daemon. If
        // you change the code here then you MUST also change the C++ code in controller's
        // main() function.
        StringBuilder prefixBuilder = new StringBuilder();
        prefixBuilder.append(namedPipeHelper.getDefaultPipeDirectoryPrefix(env)).append(Objects.requireNonNull(processName)).append('_');
        if (Strings.isNullOrEmpty(jobId) == false) {
            prefixBuilder.append(jobId).append('_');
        }
        if (uniqueId != null) {
            prefixBuilder.append(uniqueId).append('_');
        }
        String prefix = prefixBuilder.toString();
        String suffix = String.format(Locale.ROOT, "_%d", JvmInfo.jvmInfo().getPid());
        logPipeName = String.format(Locale.ROOT, "%slog%s", prefix, suffix);
        commandPipeName = wantCommandPipe ? String.format(Locale.ROOT, "%scommand%s", prefix, suffix) : null;
        processInPipeName = wantProcessInPipe ? String.format(Locale.ROOT, "%sinput%s", prefix, suffix) : null;
        processOutPipeName = wantProcessOutPipe ? String.format(Locale.ROOT, "%soutput%s", prefix, suffix) : null;
        restorePipeName = wantRestorePipe ? String.format(Locale.ROOT, "%srestore%s", prefix, suffix) : null;
        persistPipeName = wantPersistPipe ? String.format(Locale.ROOT, "%spersist%s", prefix, suffix) : null;
    }

    /**
     * Augments a list of command line arguments, for example that built up by the AutodetectBuilder class.
     */
    public void addArgs(List<String> command) {
        command.add(LOG_PIPE_ARG + logPipeName);
        if (commandPipeName != null) {
            command.add(COMMAND_PIPE_ARG + commandPipeName);
        }
        // The following are specified using two arguments, as the C++ processes could already accept input from files on disk
        if (processInPipeName != null) {
            command.add(INPUT_ARG + processInPipeName);
            command.add(INPUT_IS_PIPE_ARG);
        }
        if (processOutPipeName != null) {
            command.add(OUTPUT_ARG + processOutPipeName);
            command.add(OUTPUT_IS_PIPE_ARG);
        }
        if (restorePipeName != null) {
            command.add(RESTORE_ARG + restorePipeName);
            command.add(RESTORE_IS_PIPE_ARG);
        }
        if (persistPipeName != null) {
            command.add(PERSIST_ARG + persistPipeName);
            command.add(PERSIST_IS_PIPE_ARG);
        }
        command.add(TIMEOUT_ARG + timeout.getSeconds());
    }

    /**
     * Connect the log pipe created by the C++ process.  The must be connected before any other pipes <em>and a thread must be
     * started to read from it</em>so that there is no risk of messages logged in between creation of the other pipes on the C++
     * side from blocking due to filling up the named pipe's buffer, and hence deadlocking communications between that process
     * and this JVM.
     */
    public void connectLogStream() throws IOException {
        FileUtils.recreateTempDirectoryIfNeeded(tempDir);
        logStreamHandler = new CppLogMessageHandler(jobId, namedPipeHelper.openNamedPipeInputStream(logPipeName, timeout));
    }

    /**
     * Connect the other pipes created by the C++ process after the logging pipe has been connected.  This must be called after
     * the corresponding C++ process has been started, and after {@link #connectLogStream}.
     */
    public void connectOtherStreams() throws IOException {
        assert logStreamHandler != null : "Must connect log stream before other streams";
        if (logStreamHandler == null) {
            throw new NullPointerException("Must connect log stream before other streams");
        }
        FileUtils.recreateTempDirectoryIfNeeded(tempDir);
        // The order here is important. It must match the order that the C++ process tries to connect to the pipes, otherwise
        // a timeout is guaranteed. Also change api::CIoManager in the C++ code if changing the order here.
        try {
            if (commandPipeName != null) {
                commandStream = namedPipeHelper.openNamedPipeOutputStream(commandPipeName, timeout);
            }
            if (processInPipeName != null) {
                processInStream = namedPipeHelper.openNamedPipeOutputStream(processInPipeName, timeout);
            }
            if (processOutPipeName != null) {
                processOutStream = namedPipeHelper.openNamedPipeInputStream(processOutPipeName, timeout);
            }
            if (restorePipeName != null) {
                restoreStream = namedPipeHelper.openNamedPipeOutputStream(restorePipeName, timeout);
            }
            if (persistPipeName != null) {
                persistStream = namedPipeHelper.openNamedPipeInputStream(persistPipeName, timeout);
            }
        } catch (IOException ioe) {
            try {
                closeUnusedStreams();
            } catch (IOException suppressed) {
                ioe.addSuppressed(new IOException("Error closing process pipes", suppressed));
            }
            throw ioe;
        }
    }

    private void closeUnusedStreams() throws IOException {
        if (logStreamHandler != null) {
            logStreamHandler.close();
        }
        if (commandStream != null) {
            commandStream.close();
        }
        if (processInStream != null) {
            processInStream.close();
        }
        if (processOutStream != null) {
            processOutStream.close();
        }
        if (restoreStream != null) {
            restoreStream.close();
        }
        if (persistStream != null) {
            persistStream.close();
        }
    }

    public CppLogMessageHandler getLogStreamHandler() {
        if (logStreamHandler == null) {
            throw new IllegalStateException("process streams must be connected before use");
        }
        return logStreamHandler;
    }

    public Optional<OutputStream> getCommandStream() {
        // Distinguish between pipe not wanted and pipe wanted but not successfully connected
        if (commandPipeName == null) {
            return Optional.empty();
        }
        if (commandStream == null) {
            throw new IllegalStateException("process streams must be connected before use");
        }
        return Optional.of(commandStream);
    }

    public Optional<OutputStream> getProcessInStream() {
        // Distinguish between pipe not wanted and pipe wanted but not successfully connected
        if (processInPipeName == null) {
            return Optional.empty();
        }
        if (processInStream == null) {
            throw new IllegalStateException("process streams must be connected before use");
        }
        return Optional.of(processInStream);
    }

    public Optional<InputStream> getProcessOutStream() {
        // Distinguish between pipe not wanted and pipe wanted but not successfully connected
        if (processOutPipeName == null) {
            return Optional.empty();
        }
        if (processOutStream == null) {
            throw new IllegalStateException("process streams must be connected before use");
        }
        return Optional.of(processOutStream);
    }

    public Optional<OutputStream> getRestoreStream() {
        // Distinguish between pipe not wanted and pipe wanted but not successfully connected
        if (restorePipeName == null) {
            return Optional.empty();
        }
        if (restoreStream == null) {
            throw new IllegalStateException("process streams must be connected before use");
        }
        return Optional.of(restoreStream);
    }

    public Optional<InputStream> getPersistStream() {
        // Distinguish between pipe not wanted and pipe wanted but not successfully connected
        if (persistPipeName == null) {
            return Optional.empty();
        }
        if (persistStream == null) {
            throw new IllegalStateException("process streams must be connected before use");
        }
        return Optional.of(persistStream);
    }

    public Duration getTimeout() {
        return timeout;
    }
}
