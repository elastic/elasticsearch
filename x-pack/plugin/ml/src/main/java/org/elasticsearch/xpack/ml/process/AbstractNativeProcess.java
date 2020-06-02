/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.xpack.core.ml.MachineLearningField;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.process.logging.CppLogMessageHandler;
import org.elasticsearch.xpack.ml.process.writer.LengthEncodedWriter;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Abstract class for implementing a native process.
 */
public abstract class AbstractNativeProcess implements NativeProcess {

    private static final Logger LOGGER = LogManager.getLogger(AbstractNativeProcess.class);

    private static final Duration WAIT_FOR_KILL_TIMEOUT = Duration.ofMillis(1000);

    private final String jobId;
    private final NativeController nativeController;
    private final ProcessPipes processPipes;
    private final SetOnce<CppLogMessageHandler> cppLogHandler = new SetOnce<>();
    private final SetOnce<OutputStream> processInStream = new SetOnce<>();
    private final SetOnce<InputStream> processOutStream = new SetOnce<>();
    private final SetOnce<OutputStream> processRestoreStream = new SetOnce<>();
    private final SetOnce<LengthEncodedWriter> recordWriter = new SetOnce<>();
    private final ZonedDateTime startTime;
    private final int numberOfFields;
    private final List<Path> filesToDelete;
    private final Consumer<String> onProcessCrash;
    private final Duration processConnectTimeout;
    private volatile Future<?> logTailFuture;
    private volatile Future<?> stateProcessorFuture;
    private volatile boolean processCloseInitiated;
    private volatile boolean processKilled;
    private volatile boolean isReady;

    protected AbstractNativeProcess(String jobId, NativeController nativeController, ProcessPipes processPipes,
                                    int numberOfFields, List<Path> filesToDelete, Consumer<String> onProcessCrash,
                                    Duration processConnectTimeout) {
        this.jobId = jobId;
        this.nativeController = nativeController;
        this.processPipes = processPipes;
        this.startTime = ZonedDateTime.now();
        this.numberOfFields = numberOfFields;
        this.filesToDelete = filesToDelete;
        this.onProcessCrash = Objects.requireNonNull(onProcessCrash);
        this.processConnectTimeout = Objects.requireNonNull(processConnectTimeout);
    }

    public abstract String getName();

    /**
     * Connects the Java side of an ML process to the named pipes that connect it to the C++ side,
     * and starts tailing the C++ logs.  Stores references to all the streams except the state
     * persistence stream.
     * @param executorService the executor service to run on
     */
    public void start(ExecutorService executorService) throws IOException {

        processPipes.connectLogStream(processConnectTimeout);
        cppLogHandler.set(processPipes.getLogStreamHandler());

        logTailFuture = executorService.submit(() -> {
            try (CppLogMessageHandler h = cppLogHandler.get()) {
                h.tailStream();
            } catch (IOException e) {
                if (processKilled == false) {
                    LOGGER.error(new ParameterizedMessage("[{}] Error tailing {} process logs", jobId, getName()), e);
                }
            } finally {
                detectCrash();
            }
        });

        processPipes.connectOtherStreams(processConnectTimeout);
        if (processPipes.getProcessInStream().isPresent()) {
            processInStream.set(new BufferedOutputStream(processPipes.getProcessInStream().get()));
            this.recordWriter.set(new LengthEncodedWriter(processInStream.get()));
        }
        processOutStream.set(processPipes.getProcessOutStream().orElse(null));
        processRestoreStream.set(processPipes.getRestoreStream().orElse(null));
    }

    /**
     * Try detecting whether the process crashed i.e. stopped prematurely without any known reason.
     */
    private void detectCrash() {
        if (processCloseInitiated || processKilled) {
            // Do not detect crash when the process is being closed or killed.
            return;
        }
        if (processInStream() == null) {
            // Do not detect crash when the process has been closed automatically.
            // This is possible when the process does not have input pipe to hang on and closes right after writing its output.
            return;
        }
        // The log message doesn't say "crashed", as the process could have been killed
        // by a user or other process (e.g. the Linux OOM killer)
        String errors = cppLogHandler().getErrors();
        String fullError = String.format(Locale.ROOT, "[%s] %s process stopped unexpectedly: %s", jobId, getName(), errors);
        LOGGER.error(fullError);
        onProcessCrash.accept(fullError);
    }

    /**
     * Starts a process that may persist its state
     * @param executorService the executor service to run on
     * @param stateProcessor the state processor
     */
    public void start(ExecutorService executorService, StateProcessor stateProcessor) throws IOException {
        start(executorService);

        assert processPipes.getPersistStream().isPresent();
        stateProcessorFuture = executorService.submit(() -> {
            try (InputStream in = processPipes.getPersistStream().get()) {
                stateProcessor.process(in);
                if (processKilled == false) {
                    LOGGER.info("[{}] State output finished", jobId);
                }
            } catch (IOException e) {
                if (processKilled == false) {
                    LOGGER.error(new ParameterizedMessage("[{}] Error reading {} state output", jobId, getName()), e);
                }
            }
        });
    }

    @Override
    public boolean isReady() {
        return isReady;
    }

    protected void setReady() {
        isReady = true;
    }

    @Override
    public void writeRecord(String[] record) throws IOException {
        recordWriter().writeRecord(record);
    }

    @Override
    public void flushStream() throws IOException {
        recordWriter().flush();
    }

    @Override
    public void close() throws IOException {
        try {
            processCloseInitiated = true;
            // closing its input causes the process to exit
            if (processInStream() != null) {
                processInStream().close();
            }
            // wait for the process to exit by waiting for end-of-file on the named pipe connected
            // to the state processor - it may take a long time for all the model state to be
            // indexed
            if (stateProcessorFuture != null) {
                stateProcessorFuture.get(MachineLearningField.STATE_PERSIST_RESTORE_TIMEOUT.getMinutes(), TimeUnit.MINUTES);
            }
            // the log processor should have stopped by now too - assume processing the logs will
            // take no more than 5 seconds longer than processing the state (usually it should
            // finish first)
            if (logTailFuture != null) {
                logTailFuture.get(5, TimeUnit.SECONDS);
            }

            if (cppLogHandler().seenFatalError()) {
                throw ExceptionsHelper.serverError(cppLogHandler().getErrors());
            }
            LOGGER.debug("[{}] {} process exited", jobId, getName());
        } catch (ExecutionException | TimeoutException e) {
            LOGGER.warn(new ParameterizedMessage("[{}] Exception closing the running {} process", jobId, getName()), e);
        } catch (InterruptedException e) {
            LOGGER.warn(new ParameterizedMessage("[{}] Exception closing the running {} process", jobId, getName()), e);
            Thread.currentThread().interrupt();
        } finally {
            deleteAssociatedFiles();
        }
    }

    @Override
    public void kill() throws IOException {
        LOGGER.debug("[{}] Killing {} process", jobId, getName());
        processKilled = true;
        try {
            // The PID comes via the processes log stream. We do wait here to give the process the time to start up and report its PID.
            // Without the PID we cannot kill the process.
            nativeController.killProcess(cppLogHandler().getPid(processConnectTimeout));

            // Wait for the process to die before closing processInStream as if the process
            // is still alive when processInStream is closed it may start persisting state
            cppLogHandler().waitForLogStreamClose(WAIT_FOR_KILL_TIMEOUT);
        } catch (TimeoutException e) {
            LOGGER.warn("[{}] Failed to get PID of {} process to kill", jobId, getName());
        } finally {
            try {
                if (processInStream() != null) {
                    processInStream().close();
                }
            } catch (IOException e) {
                // Ignore it - we're shutting down and the method itself has logged a warning
            }
            try {
                deleteAssociatedFiles();
            } catch (IOException e) {
                // Ignore it - we're shutting down and the method itself has logged a warning
            }
        }
    }

    private synchronized void deleteAssociatedFiles() throws IOException {
        if (filesToDelete == null) {
            return;
        }

        for (Path fileToDelete : filesToDelete) {
            if (Files.deleteIfExists(fileToDelete)) {
                LOGGER.debug("[{}] Deleted file {}", jobId, fileToDelete.toString());
            } else {
                LOGGER.warn("[{}] Failed to delete file {}", jobId, fileToDelete.toString());
            }
        }

        filesToDelete.clear();
    }

    @Override
    public ZonedDateTime getProcessStartTime() {
        return startTime;
    }

    @Override
    public boolean isProcessAlive() {
        // Sanity check: make sure the process hasn't terminated already
        return cppLogHandler().hasLogStreamEnded() == false;
    }

    @Override
    public boolean isProcessAliveAfterWaiting() {
        cppLogHandler().waitForLogStreamClose(Duration.ofMillis(45));
        return isProcessAlive();
    }

    @Override
    public String readError() {
        return cppLogHandler().getErrors();
    }

    protected String jobId() {
        return jobId;
    }

    protected InputStream processOutStream() {
        return processOutStream.get();
    }

    @Nullable
    private OutputStream processInStream() {
        return processInStream.get();
    }

    @Nullable
    protected OutputStream processRestoreStream() {
        return processRestoreStream.get();
    }

    protected int numberOfFields() {
        return numberOfFields;
    }

    protected LengthEncodedWriter recordWriter() {
        return recordWriter.get();
    }

    protected CppLogMessageHandler cppLogHandler() {
        return cppLogHandler.get();
    }

    protected boolean isProcessKilled() {
        return processKilled;
    }

    public void consumeAndCloseOutputStream() {
        try {
            byte[] buff = new byte[512];
            while (processOutStream().read(buff) >= 0) {
                // Do nothing
            }
            processOutStream().close();
        } catch (IOException e) {
            // Given we are closing down the process there is no point propagating IO exceptions here
        }
    }
}
