/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.DataDescription;
import org.elasticsearch.xpack.ml.job.DataDescription.DataFormat;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.status.StatusReporter;
import org.elasticsearch.xpack.ml.job.transform.TransformConfigs;

import static org.mockito.Mockito.mock;

public class DataToProcessWriterFactoryTests extends ESTestCase {
    public void testCreate_GivenDataFormatIsJson() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataFormat.JSON);

        assertTrue(createWriter(dataDescription.build()) instanceof JsonDataToProcessWriter);
    }

    public void testCreate_GivenDataFormatIsCsv() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataFormat.DELIMITED);

        assertTrue(createWriter(dataDescription.build()) instanceof CsvDataToProcessWriter);
    }

    public void testCreate_GivenDataFormatIsSingleLine() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataFormat.SINGLE_LINE);

        assertTrue(createWriter(dataDescription.build()) instanceof SingleLineDataToProcessWriter);
    }

    private static DataToProcessWriter createWriter(DataDescription dataDescription) {
        return DataToProcessWriterFactory.create(true, mock(AutodetectProcess.class), dataDescription,
                mock(AnalysisConfig.class), mock(TransformConfigs.class), mock(StatusReporter.class), mock(Logger.class));
    }
}
