/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;
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

public class NativeAnalyticsProcess extends AbstractNativeAnalyticsProcess<AnalyticsResult> {

    private static final String NAME = "analytics";

    private final ProcessResultsParser<AnalyticsResult> resultsParser = new ProcessResultsParser<>(AnalyticsResult.PARSER);
    private final AnalyticsProcessConfig config;

    protected NativeAnalyticsProcess(String jobId, NativeController nativeController, InputStream logStream, OutputStream processInStream,
                                     InputStream processOutStream, OutputStream processRestoreStream, int numberOfFields,
                                     List<Path> filesToDelete, Consumer<String> onProcessCrash, AnalyticsProcessConfig config) {
        super(NAME, AnalyticsResult.PARSER, jobId, nativeController, logStream, processInStream, processOutStream, processRestoreStream,
            numberOfFields, filesToDelete, onProcessCrash);
        this.config = Objects.requireNonNull(config);
    }

    @Override
    public String getName() {
        return NAME;
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
    public Iterator<AnalyticsResult> readAnalyticsResults() {
        return resultsParser.parseResults(processOutStream());
    }

    @Override
    public AnalyticsProcessConfig getConfig() {
        return config;
    }
}
