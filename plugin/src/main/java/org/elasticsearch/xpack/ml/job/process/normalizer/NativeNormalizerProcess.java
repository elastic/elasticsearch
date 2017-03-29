/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.xpack.ml.job.process.logging.CppLogMessageHandler;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.LengthEncodedWriter;
import org.elasticsearch.xpack.ml.job.process.normalizer.output.NormalizerResultHandler;
import org.elasticsearch.xpack.ml.utils.ExceptionsHelper;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Normalizer process using native code.
 */
class NativeNormalizerProcess implements NormalizerProcess {
    private static final Logger LOGGER = Loggers.getLogger(NativeNormalizerProcess.class);

    private final String jobId;
    private final Settings settings;
    private final CppLogMessageHandler cppLogHandler;
    private final OutputStream processInStream;
    private final InputStream processOutStream;
    private final LengthEncodedWriter recordWriter;
    private volatile boolean processCloseInitiated;
    private Future<?> logTailThread;

    NativeNormalizerProcess(String jobId, Settings settings, InputStream logStream, OutputStream processInStream,
                            InputStream processOutStream, ExecutorService executorService) throws EsRejectedExecutionException {
        this.jobId = jobId;
        this.settings = settings;
        cppLogHandler = new CppLogMessageHandler(jobId, logStream);
        this.processInStream = new BufferedOutputStream(processInStream);
        this.processOutStream = processOutStream;
        this.recordWriter = new LengthEncodedWriter(this.processInStream);
        logTailThread = executorService.submit(() -> {
            try (CppLogMessageHandler h = cppLogHandler) {
                h.tailStream();
            } catch (IOException e) {
                LOGGER.error(new ParameterizedMessage("[{}] Error tailing normalizer process logs",
                        new Object[] { jobId }), e);
            } finally {
                if (processCloseInitiated == false) {
                    // The log message doesn't say "crashed", as the process could have been killed
                    // by a user or other process (e.g. the Linux OOM killer)
                    LOGGER.error("[{}] normalizer process stopped unexpectedly", jobId);
                }
            }
        });
    }

    @Override
    public void writeRecord(String[] record) throws IOException {
        recordWriter.writeRecord(record);
    }

    @Override
    public void close() throws IOException {
        try {
            processCloseInitiated = true;
            // closing its input causes the process to exit
            processInStream.close();
            // wait for the process to exit by waiting for end-of-file on the named pipe connected to its logger
            // this may take a long time as it persists the model state
            logTailThread.get(5, TimeUnit.MINUTES);
            if (cppLogHandler.seenFatalError()) {
                throw ExceptionsHelper.serverError(cppLogHandler.getErrors());
            }
            LOGGER.debug("[{}] Normalizer process exited", jobId);
        } catch (ExecutionException | TimeoutException e) {
            LOGGER.warn(new ParameterizedMessage("[{}] Exception closing the running normalizer process", new Object[] { jobId }), e);
        } catch (InterruptedException e) {
            LOGGER.warn("[{}] Exception closing the running normalizer process", jobId);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public NormalizerResultHandler createNormalizedResultsHandler() {
        return new NormalizerResultHandler(settings, processOutStream);
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
