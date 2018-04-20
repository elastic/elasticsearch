/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfigTests;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription.DataFormat;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;

import java.util.Collections;

import static org.mockito.Mockito.mock;

public class DataToProcessWriterFactoryTests extends ESTestCase {
    public void testCreate_GivenDataFormatIsJson() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataFormat.XCONTENT);

        assertTrue(createWriter(dataDescription.build()) instanceof JsonDataToProcessWriter);
    }

    public void testCreate_GivenDataFormatIsCsv() {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataFormat.DELIMITED);

        assertTrue(createWriter(dataDescription.build()) instanceof CsvDataToProcessWriter);
    }

    private static DataToProcessWriter createWriter(DataDescription dataDescription) {
        return DataToProcessWriterFactory.create(true, false, mock(AutodetectProcess.class),
                dataDescription, AnalysisConfigTests.createRandomized().build(),
                mock(DataCountsReporter.class), new NamedXContentRegistry(Collections.emptyList()));
    }
}
