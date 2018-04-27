/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.categorize;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.xpack.ml.job.process.AbstractNativeProcess;
import org.elasticsearch.xpack.ml.job.process.ProcessCtrl;
import org.elasticsearch.xpack.ml.job.process.autodetect.output.AutodetectResultsParser;
import org.elasticsearch.xpack.ml.job.results.AutodetectResult;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

class NativeCategorizeProcess extends AbstractNativeProcess implements CategorizeProcess {
    private static final Logger LOGGER = Loggers.getLogger(NativeCategorizeProcess.class);

    private final AutodetectResultsParser resultsParser;

    NativeCategorizeProcess(String jobId, InputStream logStream, OutputStream processInStream, InputStream processOutStream,
                            OutputStream processRestoreStream, int numberOfFields, List<Path> filesToDelete,
                            AutodetectResultsParser resultsParser, Runnable onProcessCrash) {
        super(ProcessCtrl.CATEGORIZE, LOGGER, jobId, logStream, processInStream, processOutStream, processRestoreStream, numberOfFields,
                filesToDelete, onProcessCrash);
        this.resultsParser = resultsParser;
    }

    @Override
    public Iterator<AutodetectResult> readResults() {
        return resultsParser.parseResults(processOutStream);
    }
}
