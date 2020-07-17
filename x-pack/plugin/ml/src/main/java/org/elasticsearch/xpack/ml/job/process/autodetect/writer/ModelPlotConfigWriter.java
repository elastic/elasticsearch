/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;

import java.io.IOException;
import java.io.Writer;
import java.util.Objects;

import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.EQUALS;
import static org.elasticsearch.xpack.ml.job.process.autodetect.writer.WriterConstants.NEW_LINE;

public class ModelPlotConfigWriter {

    private static final double BOUNDS_PERCENTILE_DEFAULT = 95.0;
    private static final double BOUNDS_PERCENTILE_DISABLE_VALUE = -1.0;

    private final ModelPlotConfig modelPlotConfig;
    private final Writer writer;

    public ModelPlotConfigWriter(ModelPlotConfig modelPlotConfig, Writer writer) {
        this.modelPlotConfig = Objects.requireNonNull(modelPlotConfig);
        this.writer = Objects.requireNonNull(writer);
    }

    public void write() throws IOException {
        StringBuilder contents = new StringBuilder();

        contents.append("boundspercentile")
                .append(EQUALS)
                .append(modelPlotConfig.isEnabled() ? 
                        BOUNDS_PERCENTILE_DEFAULT : BOUNDS_PERCENTILE_DISABLE_VALUE)
                .append(NEW_LINE);

        String terms = modelPlotConfig.getTerms();
        contents.append(ModelPlotConfig.TERMS_FIELD.getPreferredName())
                .append(EQUALS)
                .append(terms == null ? "" : terms)
                .append(NEW_LINE);

        contents.append(ModelPlotConfig.ANNOTATIONS_ENABLED_FIELD.getPreferredName())
                .append(EQUALS)
                .append(modelPlotConfig.annotationsEnabled())
                .append(NEW_LINE);

        writer.write(contents.toString());
    }
}
