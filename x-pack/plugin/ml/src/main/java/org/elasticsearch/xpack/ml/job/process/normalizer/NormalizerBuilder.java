/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.ml.job.process.ProcessBuilderUtils;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.ml.job.process.ProcessBuilderUtils.addIfNotNull;

public class NormalizerBuilder {

    /**
     * The normalization native program name - always loaded from the same directory as the controller process
     */
    public static final String NORMALIZE = "normalize";
    static final String NORMALIZE_PATH = "./" + NORMALIZE;

    private final Environment env;
    private final String jobId;
    private final String quantilesState;
    private final Integer bucketSpan;

    public NormalizerBuilder(Environment env, String jobId, String quantilesState, Integer bucketSpan) {
        this.env = env;
        this.jobId = jobId;
        this.quantilesState = quantilesState;
        this.bucketSpan = bucketSpan;
    }

    /**
     * Build the command to start the normalizer process.
     */
    public List<String> build() throws IOException {

        List<String> command = new ArrayList<>();
        command.add(NORMALIZE_PATH);
        addIfNotNull(bucketSpan, AutodetectBuilder.BUCKET_SPAN_ARG, command);
        command.add(AutodetectBuilder.LENGTH_ENCODED_INPUT_ARG);

        if (quantilesState != null) {
            Path quantilesStateFilePath = AutodetectBuilder.writeNormalizerInitState(jobId, quantilesState, env);

            String stateFileArg = AutodetectBuilder.QUANTILES_STATE_PATH_ARG + quantilesStateFilePath;
            command.add(stateFileArg);
            command.add(AutodetectBuilder.DELETE_STATE_FILES_ARG);
        }

        if (ProcessBuilderUtils.modelConfigFilePresent(env)) {
            String modelConfigFile = XPackPlugin.resolveConfigFile(env, ProcessBuilderUtils.ML_MODEL_CONF).toString();
            command.add(AutodetectBuilder.MODEL_CONFIG_ARG + modelConfigFile);
        }

        return command;
    }
}
