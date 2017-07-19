/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.ml.job.config.ModelPlotConfig;
import org.elasticsearch.xpack.ml.job.persistence.StateStreamer;
import org.elasticsearch.xpack.ml.job.process.NativeControllerHolder;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutodetectResultsParser;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.StateProcessor;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.DataLoadParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.params.FlushJobParams;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.ControlMsgToProcessWriter;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.LengthEncodedWriter;
import org.elasticsearch.xpack.ml.job.process.logging.CppLogMessageHandler;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Autodetect process using native code.
 */
class NativeAutodetectProcess implements AutodetectProcess {
    private static final Logger LOGGER = Loggers.getLogger(NativeAutodetectProcess.class);

    private static final Duration WAIT_FOR_KILL_TIMEOUT = Duration.ofMillis(1000);

    private final String jobId;
    private final CppLogMessageHandler cppLogHandler;
    private final OutputStream processInStream;
    private final InputStream processOutStream;
    private final OutputStream processRestoreStream;
    private final LengthEncodedWriter recordWriter;
    private final ZonedDateTime startTime;
    private final int numberOfAnalysisFields;
    private final List<Path> filesToDelete;
    private final Runnable onProcessCrash;
    private volatile Future<?> logTailFuture;
    private volatile Future<?> stateProcessorFuture;
    private volatile boolean processCloseInitiated;
    private volatile boolean processKilled;
    private volatile boolean isReady;
    private final AutodetectResultsParser resultsParser;

    NativeAutodetectProcess(String jobId, InputStream logStream, OutputStream processInStream,  InputStream processOutStream,
                            OutputStream processRestoreStream, int numberOfAnalysisFields, List<Path> filesToDelete,
                            AutodetectResultsParser resultsParser, Runnable onProcessCrash) {
        this.jobId = jobId;
        cppLogHandler = new CppLogMessageHandler(jobId, logStream);
        this.processInStream = new BufferedOutputStream(processInStream);
        this.processOutStream = processOutStream;
        this.processRestoreStream = processRestoreStream;
        this.recordWriter = new LengthEncodedWriter(this.processInStream);
        startTime = ZonedDateTime.now();
        this.numberOfAnalysisFields = numberOfAnalysisFields;
        this.filesToDelete = filesToDelete;
        this.resultsParser = resultsParser;
        this.onProcessCrash = Objects.requireNonNull(onProcessCrash);
    }

    public void start(ExecutorService executorService, StateProcessor stateProcessor, InputStream persistStream) {
        logTailFuture = executorService.submit(() -> {
            try (CppLogMessageHandler h = cppLogHandler) {
                h.tailStream();
            } catch (IOException e) {
                if (processKilled == false) {
                    LOGGER.error(new ParameterizedMessage("[{}] Error tailing autodetect process logs", jobId), e);
                }
            } finally {
                if (processCloseInitiated == false && processKilled == false) {
                    // The log message doesn't say "crashed", as the process could have been killed
                    // by a user or other process (e.g. the Linux OOM killer)
                    LOGGER.error("[{}] autodetect process stopped unexpectedly", jobId);
                    onProcessCrash.run();
                }
            }
        });
        stateProcessorFuture = executorService.submit(() -> {
            try (InputStream in = persistStream) {
                stateProcessor.process(jobId, in);
                if (processKilled == false) {
                    LOGGER.info("[{}] State output finished", jobId);
                }
            } catch (IOException e) {
                if (processKilled == false) {
                    LOGGER.error(new ParameterizedMessage("[{}] Error reading autodetect state output", jobId), e);
                }
            }
        });
    }

    @Override
    public void restoreState(StateStreamer stateStreamer, ModelSnapshot modelSnapshot) {
        if (modelSnapshot != null) {
            try (OutputStream r = processRestoreStream) {
                stateStreamer.restoreStateToStream(jobId, modelSnapshot, r);
            } catch (Exception e) {
                // TODO: should we fail to start?
                if (processKilled == false) {
                    LOGGER.error("Error restoring model state for job " + jobId, e);
                }
            }
        }
        isReady = true;
    }

    @Override
    public boolean isReady() {
        return isReady;
    }

    @Override
    public void writeRecord(String[] record) throws IOException {
        recordWriter.writeRecord(record);
    }

    @Override
    public void writeResetBucketsControlMessage(DataLoadParams params) throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfAnalysisFields);
        writer.writeResetBucketsMessage(params);
    }

    @Override
    public void writeUpdateModelPlotMessage(ModelPlotConfig modelPlotConfig) throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfAnalysisFields);
        writer.writeUpdateModelPlotMessage(modelPlotConfig);
    }

    @Override
    public void writeUpdateDetectorRulesMessage(int detectorIndex, List<DetectionRule> rules) throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfAnalysisFields);
        writer.writeUpdateDetectorRulesMessage(detectorIndex, rules);
    }

    @Override
    public String flushJob(FlushJobParams params) throws IOException {
        ControlMsgToProcessWriter writer = new ControlMsgToProcessWriter(recordWriter, numberOfAnalysisFields);
        writer.writeFlushControlMessage(params);
        return writer.writeFlushMessage();
    }

    @Override
    public void flushStream() throws IOException {
        recordWriter.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            processCloseInitiated = true;
            // closing its input causes the process to exit
            processInStream.close();
            // wait for the process to exit by waiting for end-of-file on the named pipe connected
            // to the state processor - it may take a long time for all the model state to be
            // indexed
            if (stateProcessorFuture != null) {
                stateProcessorFuture.get(MachineLearning.STATE_PERSIST_RESTORE_TIMEOUT.getMinutes(), TimeUnit.MINUTES);
            }
            // the log processor should have stopped by now too - assume processing the logs will
            // take no more than 5 seconds longer than processing the state (usually it should
            // finish first)
            if (logTailFuture != null) {
                logTailFuture.get(5, TimeUnit.SECONDS);
            }

            if (cppLogHandler.seenFatalError()) {
                throw ExceptionsHelper.serverError(cppLogHandler.getErrors());
            }
            LOGGER.debug("[{}] Autodetect process exited", jobId);
        } catch (ExecutionException | TimeoutException e) {
            LOGGER.warn(new ParameterizedMessage("[{}] Exception closing the running autodetect process", jobId), e);
        } catch (InterruptedException e) {
            LOGGER.warn(new ParameterizedMessage("[{}] Exception closing the running autodetect process", jobId), e);
            Thread.currentThread().interrupt();
        } finally {
            deleteAssociatedFiles();
        }
    }

    @Override
    public void kill() throws IOException {
        processKilled = true;
        try {
            // The PID comes via the processes log stream.  We don't wait for it to arrive here,
            // but if the wait times out it implies the process has only just started, in which
            // case it should die very quickly when we close its input stream.
            NativeControllerHolder.getNativeController().killProcess(cppLogHandler.getPid(Duration.ZERO));

            // Wait for the process to die before closing processInStream as if the process
            // is still alive when processInStream is closed autodetect will start persisting state
            cppLogHandler.waitForLogStreamClose(WAIT_FOR_KILL_TIMEOUT);
        } catch (TimeoutException e) {
            LOGGER.warn("[{}] Failed to get PID of autodetect process to kill", jobId);
        } finally {
            try {
                processInStream.close();
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
    public Iterator<AutodetectResult> readAutodetectResults() {
        return resultsParser.parseResults(processOutStream);
    }

    @Override
    public ZonedDateTime getProcessStartTime() {
        return startTime;
    }

    @Override
    public boolean isProcessAlive() {
        // Sanity check: make sure the process hasn't terminated already
        return !cppLogHandler.hasLogStreamEnded();
    }

    @Override
    public String readError() {
        return cppLogHandler.getErrors();
    }
}
