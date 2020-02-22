/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public class AnalyticsBuilder {

    public static final String ANALYTICS = "data_frame_analyzer";
    private static final String ANALYTICS_PATH = "./" + ANALYTICS;

    private static final String LENGTH_ENCODED_INPUT_ARG = "--lengthEncodedInput";
    private static final String CONFIG_ARG = "--config=";
    private static final String MEMORY_USAGE_ESTIMATION_ONLY_ARG = "--memoryUsageEstimationOnly";

    private final Supplier<Path> tempDirPathSupplier;
    private final NativeController nativeController;
    private final ProcessPipes processPipes;
    private final AnalyticsProcessConfig config;
    private final List<Path> filesToDelete;
    private boolean performMemoryUsageEstimationOnly;

    public AnalyticsBuilder(Supplier<Path> tempDirPathSupplier, NativeController nativeController,
                            ProcessPipes processPipes, AnalyticsProcessConfig config, List<Path> filesToDelete) {
        this.tempDirPathSupplier = Objects.requireNonNull(tempDirPathSupplier);
        this.nativeController = Objects.requireNonNull(nativeController);
        this.processPipes = Objects.requireNonNull(processPipes);
        this.config = Objects.requireNonNull(config);
        this.filesToDelete = Objects.requireNonNull(filesToDelete);
    }

    public AnalyticsBuilder performMemoryUsageEstimationOnly() {
        this.performMemoryUsageEstimationOnly = true;
        return this;
    }

    public void build() throws IOException {
        List<String> command = buildAnalyticsCommand();
        processPipes.addArgs(command);
        nativeController.startProcess(command);
    }

    private List<String> buildAnalyticsCommand() throws IOException {
        List<String> command = new ArrayList<>();
        command.add(ANALYTICS_PATH);
        command.add(LENGTH_ENCODED_INPUT_ARG);
        addConfigFile(command);
        if (performMemoryUsageEstimationOnly) {
            command.add(MEMORY_USAGE_ESTIMATION_ONLY_ARG);
        }
        return command;
    }

    private void addConfigFile(List<String> command) throws IOException {
        Path tempDir = tempDirPathSupplier.get();
        Path configFile = Files.createTempFile(tempDir, "analysis", ".conf");
        filesToDelete.add(configFile);
        try (OutputStreamWriter osw = new OutputStreamWriter(Files.newOutputStream(configFile),StandardCharsets.UTF_8);
             XContentBuilder jsonBuilder = JsonXContent.contentBuilder()) {

            config.toXContent(jsonBuilder, ToXContent.EMPTY_PARAMS);
            osw.write(Strings.toString(jsonBuilder));
        }

        command.add(CONFIG_ARG + configFile.toString());
    }
}
