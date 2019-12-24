/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.ml.process.logging.CppLogMessageHandler;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;


/**
 * Maintains the connection to the native controller daemon that can start other processes.
 */
@SuppressWarnings("ALL")
public class NativeController implements MlController {
    private static final Logger LOGGER = LogManager.getLogger(NativeController.class);

    /**
     * Process controller native program name
     */
    private static final String CONTROLLER = "controller";

    // The controller process should already be running by the time this class tries to connect to it, so the timeout
    // can be short (although there's a gotcha with EBS volumes restored from snapshot, so not TOO short)
    private static final Duration CONTROLLER_CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private static final String START_COMMAND = "start";
    private static final String KILL_COMMAND = "kill";

    private final String localNodeName;
    private final CppLogMessageHandler cppLogHandler;
    private final OutputStream commandStream;

    public static NativeController makeNativeController(String localNodeName, Environment env)
        throws IOException {
        NativeController nativeController = new NativeController(localNodeName, env, new NamedPipeHelper());
        nativeController.tailLogsInThread();
        return nativeController;
    }

    NativeController(String localNodeName, Environment env, NamedPipeHelper namedPipeHelper) throws IOException {
        ProcessPipes processPipes = new ProcessPipes(env, namedPipeHelper, CONTROLLER, null,
                true, true, false, false, false, false);
        processPipes.connectStreams(CONTROLLER_CONNECT_TIMEOUT);
        this.localNodeName = localNodeName;
        this.cppLogHandler = new CppLogMessageHandler(null, processPipes.getLogStream().get());
        this.commandStream = new BufferedOutputStream(processPipes.getCommandStream().get());
    }

    void tailLogsInThread() {
        final Thread logTailThread = new Thread(
                () -> {
                    try {
                        cppLogHandler.tailStream();
                        cppLogHandler.close();
                    } catch (IOException e) {
                        LOGGER.error("Error tailing C++ controller logs", e);
                    }
                    LOGGER.info("Native controller process has stopped - no new native processes can be started");
                },
                "ml-cpp-log-tail-thread");
        /*
         * This thread is created on the main thread so would default to being a user thread which could prevent the JVM from exiting if
         * this thread were to still be running during shutdown. As such, we mark it as a daemon thread.
         */
        logTailThread.setDaemon(true);
        logTailThread.start();
    }

    public long getPid() throws TimeoutException {
        return cppLogHandler.getPid(CONTROLLER_CONNECT_TIMEOUT);
    }

    @Override
    public Map<String, Object> getNativeCodeInfo() throws TimeoutException {
        return cppLogHandler.getNativeCodeInfo(CONTROLLER_CONNECT_TIMEOUT);
    }

    public void startProcess(List<String> command) throws IOException {
        if (command.isEmpty()) {
            throw new IllegalArgumentException("Cannot start process: no command supplied");
        }

        // Sanity check to avoid hard-to-debug errors - tabs and newlines will confuse the controller process
        for (String arg : command) {
            if (arg.contains("\t")) {
                throw new IllegalArgumentException("argument contains a tab character: " + arg + " in " + command);
            }
            if (arg.contains("\n")) {
                throw new IllegalArgumentException("argument contains a newline character: " + arg + " in " + command);
            }
        }

        if (cppLogHandler.hasLogStreamEnded()) {
            String msg = "Cannot start process [" + command.get(0) + "]: native controller process has stopped on node ["
                + localNodeName + "]";
            LOGGER.error(msg);
            throw new ElasticsearchException(msg);
        }

        synchronized (commandStream) {
            LOGGER.debug("Starting process with command: " + command);
            commandStream.write(START_COMMAND.getBytes(StandardCharsets.UTF_8));
            for (String arg : command) {
                commandStream.write('\t');
                commandStream.write(arg.getBytes(StandardCharsets.UTF_8));
            }
            commandStream.write('\n');
            commandStream.flush();
        }
    }

    public void killProcess(long pid) throws TimeoutException, IOException {
        if (pid <= 0) {
            throw new IllegalArgumentException("invalid PID to kill: " + pid);
        }
        if (pid == getPid()) {
            throw new IllegalArgumentException("native controller will not kill self: " + pid);
        }

        if (cppLogHandler.hasLogStreamEnded()) {
            String msg = "Cannot kill process with PID [" + pid + "]: native controller process has stopped on node ["
                + localNodeName + "]";
            LOGGER.error(msg);
            throw new ElasticsearchException(msg);
        }

        synchronized (commandStream) {
            LOGGER.debug("Killing process with PID: " + pid);
            commandStream.write(KILL_COMMAND.getBytes(StandardCharsets.UTF_8));
            commandStream.write('\t');
            commandStream.write(Long.toString(pid).getBytes(StandardCharsets.UTF_8));
            commandStream.write('\n');
            commandStream.flush();
        }
    }

    @Override
    public void stop() throws IOException {
        // The C++ process will exit when it gets EOF on the command stream
        commandStream.close();
    }
}
