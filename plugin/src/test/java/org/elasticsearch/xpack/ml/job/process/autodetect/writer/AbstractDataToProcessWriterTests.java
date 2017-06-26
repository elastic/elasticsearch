/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.AbstractDataToProcessWriter.InputOutputMap;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Testing methods of AbstractDataToProcessWriter but uses the concrete instances.
 */
public class AbstractDataToProcessWriterTests extends ESTestCase {
    private AutodetectProcess autodetectProcess;
    private DataCountsReporter dataCountsReporter;

    @Before
    public void setUpMocks() {
        autodetectProcess = Mockito.mock(AutodetectProcess.class);
        dataCountsReporter = Mockito.mock(DataCountsReporter.class);
    }

    public void testInputFields() throws IOException {
        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setTimeField("time_field");

        Detector.Builder detector = new Detector.Builder("metric", "value");
        detector.setByFieldName("metric");
        detector.setPartitionFieldName("host");
        detector.setDetectorDescription("metric(value) by metric partitionfield=host");
        AnalysisConfig ac = new AnalysisConfig.Builder(Arrays.asList(detector.build())).build();

        AbstractDataToProcessWriter writer =
                new CsvDataToProcessWriter(true, autodetectProcess, dd.build(), ac, dataCountsReporter);

        writer.writeHeader();

        Set<String> inputFields = new HashSet<>(writer.inputFields());
        assertEquals(4, inputFields.size());
        assertTrue(inputFields.contains("time_field"));
        assertTrue(inputFields.contains("value"));
        assertTrue(inputFields.contains("host"));
        assertTrue(inputFields.contains("metric"));

        String[] header = { "time_field", "metric", "host", "value" };
        writer.buildFieldIndexMapping(header);

        Map<String, Integer> inputIndexes = writer.getInputFieldIndexes();
        assertEquals(4, inputIndexes.size());
        assertEquals(new Integer(0), inputIndexes.get("time_field"));
        assertEquals(new Integer(1), inputIndexes.get("metric"));
        assertEquals(new Integer(2), inputIndexes.get("host"));
        assertEquals(new Integer(3), inputIndexes.get("value"));

        Map<String, Integer> outputIndexes = writer.getOutputFieldIndexes();
        assertEquals(5, outputIndexes.size());
        assertEquals(new Integer(0), outputIndexes.get("time_field"));
        assertEquals(new Integer(1), outputIndexes.get("host"));
        assertEquals(new Integer(2), outputIndexes.get("metric"));
        assertEquals(new Integer(3), outputIndexes.get("value"));
        assertEquals(new Integer(4), outputIndexes.get(LengthEncodedWriter.CONTROL_FIELD_NAME));

        List<InputOutputMap> inOutMaps = writer.getInputOutputMap();
        assertEquals(4, inOutMaps.size());
        assertEquals(inOutMaps.get(0).inputIndex, 0);
        assertEquals(inOutMaps.get(0).outputIndex, 0);
        assertEquals(inOutMaps.get(1).inputIndex, 2);
        assertEquals(inOutMaps.get(1).outputIndex, 1);
        assertEquals(inOutMaps.get(2).inputIndex, 1);
        assertEquals(inOutMaps.get(2).outputIndex, 2);
        assertEquals(inOutMaps.get(3).inputIndex, 3);
        assertEquals(inOutMaps.get(3).outputIndex, 3);
    }
}
