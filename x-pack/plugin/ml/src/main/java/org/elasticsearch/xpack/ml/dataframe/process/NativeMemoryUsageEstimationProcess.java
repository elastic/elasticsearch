/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.ml.dataframe.process.results.MemoryUsageEstimationResult;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;

import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;

public class NativeMemoryUsageEstimationProcess extends AbstractNativeAnalyticsProcess<MemoryUsageEstimationResult> {

    private static final String NAME = "memory_usage_estimation";

    protected NativeMemoryUsageEstimationProcess(
        String jobId,
        NativeController nativeController,
        ProcessPipes processPipes,
        int numberOfFields,
        List<Path> filesToDelete,
        Consumer<String> onProcessCrash
    ) {
        super(
            NAME,
            MemoryUsageEstimationResult.PARSER,
            jobId,
            nativeController,
            processPipes,
            numberOfFields,
            filesToDelete,
            onProcessCrash,
            NamedXContentRegistry.EMPTY
        );
    }

    @Override
    public AnalyticsProcessConfig getConfig() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void restoreState(Client client, String stateDocIdPrefix) {
        throw new UnsupportedOperationException();
    }
}
