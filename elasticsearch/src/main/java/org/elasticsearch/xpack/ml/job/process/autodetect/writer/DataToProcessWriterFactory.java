/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.DataDescription;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.status.StatusReporter;
import org.elasticsearch.xpack.ml.job.transform.TransformConfigs;

/**
 * Factory for creating the suitable writer depending on
 * whether the data format is JSON or not, and on the kind
 * of date transformation that should occur.
 */
public final class DataToProcessWriterFactory {

    private DataToProcessWriterFactory() {

    }

    /**
     * Constructs a {@link DataToProcessWriter} depending on
     * the data format and the time transformation.
     *
     * @return A {@link JsonDataToProcessWriter} if the data
     * format is JSON or otherwise a {@link CsvDataToProcessWriter}
     */
    public static DataToProcessWriter create(boolean includeControlField, AutodetectProcess autodetectProcess,
                                             DataDescription dataDescription, AnalysisConfig analysisConfig,
                                             TransformConfigs transforms, StatusReporter statusReporter, Logger logger) {
        switch (dataDescription.getFormat()) {
        case JSON:
            return new JsonDataToProcessWriter(includeControlField, autodetectProcess, dataDescription, analysisConfig,
                    transforms, statusReporter, logger);
        case DELIMITED:
            return new CsvDataToProcessWriter(includeControlField, autodetectProcess, dataDescription, analysisConfig,
                    transforms, statusReporter, logger);
        case SINGLE_LINE:
            return new SingleLineDataToProcessWriter(includeControlField, autodetectProcess, dataDescription, analysisConfig,
                    transforms, statusReporter, logger);
        default:
            throw new IllegalArgumentException();
        }
    }
}
