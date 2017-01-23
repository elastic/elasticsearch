/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.xpack.ml.job.config.ModelDebugConfig;

import java.io.IOException;
import java.io.Writer;
import java.util.Objects;

import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.EQUALS;
import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.NEW_LINE;

public class ModelDebugConfigWriter {
    private static final String WRITE_TO_STR = "writeto";
    private static final String BOUNDS_PERCENTILE_STR = "boundspercentile";
    private static final String TERMS_STR = "terms";

    private final ModelDebugConfig modelDebugConfig;
    private final Writer writer;

    public ModelDebugConfigWriter(ModelDebugConfig modelDebugConfig, Writer writer) {
        this.modelDebugConfig = Objects.requireNonNull(modelDebugConfig);
        this.writer = Objects.requireNonNull(writer);
    }

    public void write() throws IOException {
        StringBuilder contents = new StringBuilder();
        if (modelDebugConfig.getWriteTo() != null) {
            contents.append(WRITE_TO_STR)
            .append(EQUALS)
            .append(modelDebugConfig.getWriteTo())
            .append(NEW_LINE);
        }

        contents.append(BOUNDS_PERCENTILE_STR)
        .append(EQUALS)
        .append(modelDebugConfig.getBoundsPercentile())
        .append(NEW_LINE);

        String terms = modelDebugConfig.getTerms();
        contents.append(TERMS_STR)
        .append(EQUALS)
        .append(terms == null ? "" : terms)
        .append(NEW_LINE);

        writer.write(contents.toString());
    }
}
