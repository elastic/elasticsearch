/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.ml.job.process.logging.CppLogMessageHandler;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;


/**
 * Maintains the connection to the native controller daemon that can start other processes.
 */
public class NativeController {
    private static final Logger LOGGER = Loggers.getLogger(NativeController.class);

    // TODO: this can be reduced once Elasticsearch is automatically starting the controller process -
    // at the moment it has to be started manually, which could take a while
    private static final Duration CONTROLLER_CONNECT_TIMEOUT = Duration.ofMinutes(1);

    private static final String START_COMMAND = "start";

    private final CppLogMessageHandler cppLogHandler;
    private final OutputStream commandStream;
    private Thread logTailThread;

    public NativeController(Environment env, NamedPipeHelper namedPipeHelper) throws IOException {
        ProcessPipes processPipes = new ProcessPipes(env, namedPipeHelper, ProcessCtrl.CONTROLLER, null,
                true, true, false, false, false, false);
        processPipes.connectStreams(CONTROLLER_CONNECT_TIMEOUT);
        cppLogHandler = new CppLogMessageHandler(null, processPipes.getLogStream().get());
        commandStream = processPipes.getCommandStream().get();
    }

    public void tailLogsInThread() {
        logTailThread = new Thread(() -> {
            try {
                cppLogHandler.tailStream();
                cppLogHandler.close();
            } catch (IOException e) {
                LOGGER.error("Error tailing C++ controller logs", e);
            }
            LOGGER.info("Native controller process has stopped - no new native processes can be started");
        });
        logTailThread.start();
    }

    public long getPid() throws TimeoutException {
        return cppLogHandler.getPid(CONTROLLER_CONNECT_TIMEOUT);
    }

    public void startProcess(List<String> command) throws IOException {
        // Sanity check to avoid hard-to-debug errors - tabs and newlines will confuse the controller process
        for (String arg : command) {
            if (arg.contains("\t")) {
                throw new IllegalArgumentException("argument contains a tab character: " + arg + " in " + command);
            }
            if (arg.contains("\n")) {
                throw new IllegalArgumentException("argument contains a newline character: " + arg + " in " + command);
            }
        }

        synchronized (commandStream) {
            LOGGER.debug("Starting process with command: " + command);
            commandStream.write(START_COMMAND.getBytes(StandardCharsets.UTF_8));
            for (String arg : command) {
                commandStream.write('\t');
                commandStream.write(arg.getBytes(StandardCharsets.UTF_8));
            }
            commandStream.write('\n');
        }
    }
}
