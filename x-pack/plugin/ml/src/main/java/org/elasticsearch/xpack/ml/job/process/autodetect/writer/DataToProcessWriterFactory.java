/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;

/**
 * Factory for creating the suitable writer depending on whether the data format
 * is JSON or not, and on the kind of date transformation that should occur.
 */
public final class DataToProcessWriterFactory {

    private DataToProcessWriterFactory() {
    }

    /**
     * Constructs a {@link DataToProcessWriter} depending on the data format and
     * the time transformation.
     *
     * @return A {@link JsonDataToProcessWriter} if the data format is JSON or
     *         otherwise a {@link CsvDataToProcessWriter}
     */
    public static DataToProcessWriter create(boolean includeControlField, boolean includeTokensField,
            AutodetectProcess autodetectProcess, DataDescription dataDescription,
            AnalysisConfig analysisConfig, DataCountsReporter dataCountsReporter,
            NamedXContentRegistry xContentRegistry) {
        switch (dataDescription.getFormat()) {
        case XCONTENT:
            return new JsonDataToProcessWriter(includeControlField, includeTokensField, autodetectProcess,
                    dataDescription, analysisConfig, dataCountsReporter, xContentRegistry);
        case DELIMITED:
            return new CsvDataToProcessWriter(includeControlField, includeTokensField, autodetectProcess,
                    dataDescription, analysisConfig, dataCountsReporter);
        default:
            throw new IllegalArgumentException();
        }
    }
}
