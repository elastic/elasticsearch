/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.analytics.process;

import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AnalyticsBuilder {

    public static final String ANALYTICS = "data_frame_analyzer";
    private static final String ANALYTICS_PATH = "./" + ANALYTICS;

    private static final String LENGTH_ENCODED_INPUT_ARG = "--lengthEncodedInput";

    private final NativeController nativeController;
    private final ProcessPipes processPipes;

    public AnalyticsBuilder(NativeController nativeController, ProcessPipes processPipes) {
        this.nativeController = Objects.requireNonNull(nativeController);
        this.processPipes = Objects.requireNonNull(processPipes);
    }

    public void build() throws IOException {
        List<String> command = buildAnalyticsCommand();
        processPipes.addArgs(command);
        nativeController.startProcess(command);
    }

    List<String> buildAnalyticsCommand() {
        List<String> command = new ArrayList<>();
        command.add(ANALYTICS_PATH);
        command.add(LENGTH_ENCODED_INPUT_ARG);
        return command;
    }
}
