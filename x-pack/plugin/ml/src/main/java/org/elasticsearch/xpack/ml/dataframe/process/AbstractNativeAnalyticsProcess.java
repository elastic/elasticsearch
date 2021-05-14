/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.ml.process.AbstractNativeProcess;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.process.ProcessResultsParser;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

abstract class AbstractNativeAnalyticsProcess<Result> extends AbstractNativeProcess implements AnalyticsProcess<Result> {

    private final String name;
    private final ProcessResultsParser<Result> resultsParser;

    protected AbstractNativeAnalyticsProcess(String name, ConstructingObjectParser<Result, Void> resultParser, String jobId,
                                             NativeController nativeController, ProcessPipes processPipes,
                                             int numberOfFields, List<Path> filesToDelete, Consumer<String> onProcessCrash,
                                             NamedXContentRegistry namedXContentRegistry) {
        super(jobId, nativeController, processPipes, numberOfFields, filesToDelete, onProcessCrash);
        this.name = Objects.requireNonNull(name);
        this.resultsParser = new ProcessResultsParser<>(Objects.requireNonNull(resultParser), namedXContentRegistry);
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
    public void persistState(long timestamp, String id, String description) {
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

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            // Unlike autodetect where closing the process input stream initiates
            // termination and additional output from the process which forces us
            // to close the output stream after we've finished processing its results,
            // in analytics we wait until we've read all results and then we close the
            // process. Thus, we can take care of consuming and closing the output
            // stream within close itself.
            consumeAndCloseOutputStream();
        }
    }
}
