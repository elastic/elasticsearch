/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.ml.process.logging.CppLogMessageHandler;
import org.elasticsearch.xpack.ml.utils.NamedPipeHelper;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
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
    private final InputStream responseStream;
    private final NamedXContentRegistry xContentRegistry;
    private final Map<Integer, ResponseTracker> responseTrackers = new ConcurrentHashMap<>();
    // The response iterator cannot be constructed until something is expected to be in the stream it's reading from,
    // otherwise it will block while it tries to read a few bytes to determine the character set. It could be created
    // immediately in a dedicated thread, but that's wasteful as we can reuse the threads that are sending the commands
    // to the controller to read the responses. So we create it in the first thread that wants to know the response to
    // a command.
    private final SetOnce<Iterator<ControllerResponse>> responseIteratorHolder = new SetOnce<>();
    private int nextCommandId = 1; // synchronized on commandStream so doesn't need to be volatile

    public static NativeController makeNativeController(String localNodeName, Environment env, NamedXContentRegistry xContentRegistry)
        throws IOException {
        return new NativeController(localNodeName, env, new NamedPipeHelper(), xContentRegistry);
    }

    NativeController(String localNodeName, Environment env, NamedPipeHelper namedPipeHelper, NamedXContentRegistry xContentRegistry)
        throws IOException {
        this.localNodeName = localNodeName;
        ProcessPipes processPipes = new ProcessPipes(
            env,
            namedPipeHelper,
            CONTROLLER_CONNECT_TIMEOUT,
            CONTROLLER,
            null,
            null,
            true,
            false,
            true,
            false,
            false
        );
        processPipes.connectLogStream();
        this.cppLogHandler = processPipes.getLogStreamHandler();
        tailLogsInThread(cppLogHandler);
        processPipes.connectOtherStreams();
        this.commandStream = new BufferedOutputStream(processPipes.getCommandStream().get());
        this.responseStream = processPipes.getProcessOutStream().get();
        this.xContentRegistry = xContentRegistry;
    }

    static void tailLogsInThread(CppLogMessageHandler cppLogHandler) {
        final Thread logTailThread = new Thread(() -> {
            try (CppLogMessageHandler h = cppLogHandler) {
                h.tailStream();
            } catch (IOException e) {
                LOGGER.error("Error tailing C++ controller logs", e);
            }
            LOGGER.info("Native controller process has stopped - no new native processes can be started");
        }, "ml-cpp-log-tail-thread");
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

    public void startProcess(List<String> command) throws IOException, InterruptedException {
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
            String msg = "Cannot start process ["
                + command.get(0)
                + "]: native controller process has stopped on node ["
                + localNodeName
                + "]";
            LOGGER.error(msg);
            throw new ElasticsearchException(msg);
        }

        int commandId = -1;
        try {
            synchronized (commandStream) {
                commandId = nextCommandId++;
                setupResponseTracker(commandId);
                LOGGER.debug("Command [{}]: starting process with command {}", commandId, command);
                commandStream.write(Integer.toString(commandId).getBytes(StandardCharsets.UTF_8));
                commandStream.write('\t');
                commandStream.write(START_COMMAND.getBytes(StandardCharsets.UTF_8));
                for (String arg : command) {
                    commandStream.write('\t');
                    commandStream.write(arg.getBytes(StandardCharsets.UTF_8));
                }
                commandStream.write('\n');
                commandStream.flush();
            }
            awaitCompletion(commandId);
        } finally {
            removeResponseTracker(commandId);
        }
    }

    public void killProcess(long pid, boolean awaitCompletion) throws TimeoutException, IOException, InterruptedException {
        if (pid <= 0) {
            throw new IllegalArgumentException("invalid PID to kill: " + pid);
        }
        if (pid == getPid()) {
            throw new IllegalArgumentException("native controller will not kill self: " + pid);
        }

        if (cppLogHandler.hasLogStreamEnded()) {
            String msg = "Cannot kill process with PID ["
                + pid
                + "]: native controller process has stopped on node ["
                + localNodeName
                + "]";
            LOGGER.error(msg);
            throw new ElasticsearchException(msg);
        }

        int commandId = -1;
        try {
            synchronized (commandStream) {
                commandId = nextCommandId++;
                if (awaitCompletion) {
                    setupResponseTracker(commandId);
                }
                LOGGER.debug("Command [{}]: killing process with PID [{}]", commandId, pid);
                commandStream.write(Integer.toString(commandId).getBytes(StandardCharsets.UTF_8));
                commandStream.write('\t');
                commandStream.write(KILL_COMMAND.getBytes(StandardCharsets.UTF_8));
                commandStream.write('\t');
                commandStream.write(Long.toString(pid).getBytes(StandardCharsets.UTF_8));
                commandStream.write('\n');
                commandStream.flush();
            }
            if (awaitCompletion) {
                awaitCompletion(commandId);
            }
        } finally {
            if (awaitCompletion) {
                removeResponseTracker(commandId);
            }
        }
    }

    @Override
    public void stop() throws IOException {
        // The C++ process will exit when it gets EOF on the command stream
        commandStream.close();
    }

    private void setupResponseTracker(int commandId) {
        ResponseTracker tracker = new ResponseTracker();
        ResponseTracker previous = responseTrackers.put(commandId, tracker);
        assert previous == null;
    }

    private void removeResponseTracker(int commandId) {
        responseTrackers.remove(commandId);
    }

    private void awaitCompletion(int commandId) throws IOException, InterruptedException {

        ResponseTracker ourResponseTracker = responseTrackers.get(commandId);
        assert ourResponseTracker != null;

        // If our response has not been seen already (by another thread), parse messages under lock until it is seen.
        // This approach means that of all the threads waiting for controller responses, one is parsing the messages
        // on behalf of all of them, and the others are blocked. When the thread that is parsing gets the response
        // it needs another thread will pick up the parsing.
        if (ourResponseTracker.hasResponded() == false) {
            synchronized (responseIteratorHolder) {
                Iterator<ControllerResponse> responseIterator = responseIteratorHolder.get();
                if (responseIterator == null) {
                    responseIterator = new ProcessResultsParser<ControllerResponse>(ControllerResponse.PARSER, xContentRegistry)
                        .parseResults(this.responseStream);
                    responseIteratorHolder.set(responseIterator);
                }
                while (ourResponseTracker.hasResponded() == false) {
                    if (responseIterator.hasNext() == false) {
                        throw new IOException(
                            "ML controller response stream ended while awaiting response for command [" + commandId + "]"
                        );
                    }
                    ControllerResponse response = responseIterator.next();
                    ResponseTracker respondedTracker = responseTrackers.get(response.getCommandId());
                    // It is not compulsory to track all responses, hence legitimate not to find every ID in the map
                    if (respondedTracker != null) {
                        respondedTracker.setResponse(response);
                    }
                }
            }
        }

        ControllerResponse ourResponse = ourResponseTracker.getResponse();
        assert ourResponse.getCommandId() == commandId;
        if (ourResponse.isSuccess()) {
            LOGGER.debug("ML controller successfully executed command [" + commandId + "]: [" + ourResponse.getReason() + "]");
        } else {
            throw new IOException("ML controller failed to execute command [" + commandId + "]: [" + ourResponse.getReason() + "]");
        }
    }

    private static class ResponseTracker {

        private final CountDownLatch latch = new CountDownLatch(1);
        private final SetOnce<ControllerResponse> responseHolder = new SetOnce<>();

        boolean hasResponded() {
            return latch.getCount() < 1;
        }

        void setResponse(ControllerResponse response) {
            responseHolder.set(response);
            latch.countDown();
        }

        ControllerResponse getResponse() throws InterruptedException {
            latch.await();
            return responseHolder.get();
        }
    }
}
