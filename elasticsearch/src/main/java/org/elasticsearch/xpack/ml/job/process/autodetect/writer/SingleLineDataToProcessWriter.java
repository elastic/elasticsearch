/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.DataCounts;
import org.elasticsearch.xpack.ml.job.DataDescription;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.status.StatusReporter;
import org.elasticsearch.xpack.ml.job.transform.TransformConfigs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * This writer is used for reading inputIndex data that are unstructured and
 * each record is a single line. The writer applies transforms and pipes
 * the records into length encoded outputIndex.
 * <p>
 * This writer is expected only to be used in combination of transforms
 * that will extract the time and the other fields used in the analysis.
 * <p>
 * Records for which no time can be extracted will be ignored.
 */
public class SingleLineDataToProcessWriter extends AbstractDataToProcessWriter {
    private static final String RAW = "raw";

    protected SingleLineDataToProcessWriter(boolean includeControlField, AutodetectProcess autodetectProcess,
            DataDescription dataDescription, AnalysisConfig analysisConfig,
            TransformConfigs transformConfigs, StatusReporter statusReporter, Logger logger) {
        super(includeControlField, autodetectProcess, dataDescription, analysisConfig, transformConfigs, statusReporter, logger);
    }

    @Override
    public DataCounts write(InputStream inputStream) throws IOException {
        statusReporter.startNewIncrementalCount();

        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String[] header = {RAW};
            buildTransforms(header);

            int numFields = outputFieldCount();
            String[] record = new String[numFields];

            for (String line = bufferedReader.readLine(); line != null;
                    line = bufferedReader.readLine()) {
                Arrays.fill(record, "");
                applyTransformsAndWrite(new String[]{line}, record, 1);
            }
            statusReporter.finishReporting();
        }

        return statusReporter.incrementalStats();
    }

    @Override
    protected boolean checkForMissingFields(Collection<String> inputFields,
            Map<String, Integer> inputFieldIndexes, String[] header) {
        return true;
    }
}
