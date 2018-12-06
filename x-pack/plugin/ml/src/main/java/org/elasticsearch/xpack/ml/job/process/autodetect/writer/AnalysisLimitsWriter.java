/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Objects;

import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.EQUALS;
import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.NEW_LINE;

public class AnalysisLimitsWriter {
    /*
     * The configuration fields used in limits.conf
     */
    private static final String MEMORY_STANZA_STR = "[memory]";
    private static final String RESULTS_STANZA_STR = "[results]";
    private static final String MODEL_MEMORY_LIMIT_CONFIG_STR = "modelmemorylimit";
    private static final String MAX_EXAMPLES_LIMIT_CONFIG_STR = "maxexamples";

    private final AnalysisLimits limits;
    private final OutputStreamWriter writer;

    public AnalysisLimitsWriter(AnalysisLimits limits, OutputStreamWriter writer) {
        this.limits = Objects.requireNonNull(limits);
        this.writer = Objects.requireNonNull(writer);
    }

    public void write() throws IOException {
        StringBuilder contents = new StringBuilder(MEMORY_STANZA_STR).append(NEW_LINE);
        if (limits.getModelMemoryLimit() != null) {
            contents.append(MODEL_MEMORY_LIMIT_CONFIG_STR + EQUALS).append(limits.getModelMemoryLimit()).append(NEW_LINE);
        }

        contents.append(RESULTS_STANZA_STR).append(NEW_LINE);
        if (limits.getCategorizationExamplesLimit() != null) {
            contents.append(MAX_EXAMPLES_LIMIT_CONFIG_STR + EQUALS).append(limits.getCategorizationExamplesLimit()).append(NEW_LINE);
        }

        writer.write(contents.toString());
    }
}
