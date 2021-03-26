/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.inference.deployment.PyTorchResult;
import org.elasticsearch.xpack.ml.process.AbstractNativeProcess;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.elasticsearch.xpack.ml.process.ProcessResultsParser;
import org.elasticsearch.xpack.ml.process.writer.LengthEncodedWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class NativePyTorchProcess extends AbstractNativeProcess implements PyTorchProcess {

    private static final Logger logger = LogManager.getLogger(NativePyTorchProcess.class);

    private static final String NAME = "pytorch_inference";

    private static AtomicLong ms_RequestId = new AtomicLong(1);

    private final ProcessResultsParser<PyTorchResult> resultsParser;

    protected NativePyTorchProcess(String jobId, NativeController nativeController, ProcessPipes processPipes, int numberOfFields,
                                   List<Path> filesToDelete, Consumer<String> onProcessCrash) {
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
    public void loadModel(String modelBase64, int modelSizeAfterUnbase64) throws IOException {
        byte[] modelBytes = Base64.getDecoder().decode(modelBase64.getBytes(StandardCharsets.UTF_8));
        try (OutputStream restoreStream = processRestoreStream()) {
            LengthEncodedWriter lengthEncodedWriter = new LengthEncodedWriter(restoreStream);
            lengthEncodedWriter.writeNumFields(modelSizeAfterUnbase64);
            restoreStream.write(modelBytes);
        }
    }

    @Override
    public Iterator<PyTorchResult> readResults() {
        return resultsParser.parseResults(processOutStream());
    }

    @Override
    public String writeInferenceRequest(double[] inputs) throws IOException {
        long requestId = ms_RequestId.getAndIncrement();
        String json = new StringBuilder("{")
            .append("\"request_id\":\"")
            .append(requestId)
            .append("\",")
            .append("\"inputs\":")
            .append(Arrays.toString(inputs))
            .append("}\n")
            .toString();

        processInStream().write(json.getBytes(StandardCharsets.UTF_8));
        processInStream().flush();

        return String.valueOf(requestId);
    }
}
