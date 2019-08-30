/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.xpack.ml.process.AbstractNativeProcess;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessResultsParser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

abstract class AbstractNativeAnalyticsProcess<Result> extends AbstractNativeProcess implements AnalyticsProcess<Result> {

    private final String name;
    private final ProcessResultsParser<Result> resultsParser;

    protected AbstractNativeAnalyticsProcess(String name, ConstructingObjectParser<Result, Void> resultParser, String jobId,
                                             NativeController nativeController, InputStream logStream, OutputStream processInStream,
                                             InputStream processOutStream, OutputStream processRestoreStream, int numberOfFields,
                                             List<Path> filesToDelete, Consumer<String> onProcessCrash) {
        super(jobId, nativeController, logStream, processInStream, processOutStream, processRestoreStream, numberOfFields, filesToDelete,
            onProcessCrash);
        this.name = Objects.requireNonNull(name);
        this.resultsParser = new ProcessResultsParser<>(Objects.requireNonNull(resultParser));
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void persistState() {
        // Nothing to persist
    }

    @Override
    public void writeEndOfDataMessage() throws IOException {
        new AnalyticsControlMessageWriter(recordWriter(), numberOfFields()).writeEndOfData();
    }

    @Override
    public Iterator<Result> readAnalyticsResults() {
        return resultsParser.parseResults(processOutStream());
    }
}
