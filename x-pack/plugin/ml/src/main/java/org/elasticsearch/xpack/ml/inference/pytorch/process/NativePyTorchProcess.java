/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.ml.inference.pytorch.results.PyTorchResult;
import org.elasticsearch.xpack.ml.process.AbstractNativeProcess;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.process.ProcessResultsParser;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class NativePyTorchProcess extends AbstractNativeProcess implements PyTorchProcess {

    private static final String NAME = "pytorch_inference";

    private final ProcessResultsParser<PyTorchResult> resultsParser;

    protected NativePyTorchProcess(
        String jobId,
        NativeController nativeController,
        ProcessPipes processPipes,
        int numberOfFields,
        List<Path> filesToDelete,
        Consumer<String> onProcessCrash
    ) {
        super(jobId, nativeController, processPipes, numberOfFields, filesToDelete, onProcessCrash);
        this.resultsParser = new ProcessResultsParser<>(PyTorchResult.PARSER, NamedXContentRegistry.EMPTY);
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public void persistState() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void persistState(long snapshotTimestampMs, String snapshotId, String snapshotDescription) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadModel(String modelId, String index, PyTorchStateStreamer stateStreamer, ActionListener<Boolean> listener) {
        stateStreamer.writeStateToStream(modelId, index, processRestoreStream(), listener);
    }

    @Override
    public Iterator<PyTorchResult> readResults() {
        return resultsParser.parseResults(processOutStream());
    }

    @Override
    public void writeInferenceRequest(BytesReference jsonRequest) throws IOException {
        processInStream().write(jsonRequest.array(), jsonRequest.arrayOffset(), jsonRequest.length());
        processInStream().write('\n');
        processInStream().flush();
    }
}
