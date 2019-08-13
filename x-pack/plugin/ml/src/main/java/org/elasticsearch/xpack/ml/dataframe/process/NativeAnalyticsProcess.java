/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.xpack.ml.dataframe.process.results.AnalyticsResult;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

public class NativeAnalyticsProcess extends AbstractNativeAnalyticsProcess<AnalyticsResult> {

    private static final String NAME = "analytics";

    protected NativeAnalyticsProcess(String jobId, InputStream logStream, OutputStream processInStream,
                                     InputStream processOutStream, OutputStream processRestoreStream, int numberOfFields,
                                     List<Path> filesToDelete, Consumer<String> onProcessCrash) {
        super(NAME, AnalyticsResult.PARSER, jobId, logStream, processInStream, processOutStream, processRestoreStream, numberOfFields,
            filesToDelete, onProcessCrash);
    }
}
