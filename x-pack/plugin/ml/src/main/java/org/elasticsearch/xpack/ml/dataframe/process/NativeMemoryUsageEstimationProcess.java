/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.ml.dataframe.process.results.MemoryUsageEstimationResult;
import org.elasticsearch.xpack.ml.process.NativeController;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

public class NativeMemoryUsageEstimationProcess extends AbstractNativeAnalyticsProcess<MemoryUsageEstimationResult> {

    private static final String NAME = "memory_usage_estimation";

    protected NativeMemoryUsageEstimationProcess(String jobId, NativeController nativeController, InputStream logStream,
                                                 OutputStream processInStream, InputStream processOutStream,
                                                 OutputStream processRestoreStream, int numberOfFields, List<Path> filesToDelete,
                                                 Consumer<String> onProcessCrash, Duration processConnectTimeout) {
        super(NAME, MemoryUsageEstimationResult.PARSER, jobId, nativeController, logStream, processInStream, processOutStream,
            processRestoreStream, numberOfFields, filesToDelete, onProcessCrash, processConnectTimeout, NamedXContentRegistry.EMPTY);
    }

    @Override
    public AnalyticsProcessConfig getConfig() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restoreState(BytesReference state) {
        throw new UnsupportedOperationException();
    }
}
