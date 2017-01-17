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
import org.elasticsearch.xpack.ml.job.Detector;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.status.StatusReporter;
import org.elasticsearch.xpack.ml.job.transform.TransformConfig;
import org.elasticsearch.xpack.ml.job.transform.TransformConfigs;
import org.elasticsearch.xpack.ml.job.transform.TransformType;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

public class DataWithTransformsToProcessWriterTests extends ESTestCase {
    private AutodetectProcess autodetectProcess;
    private StatusReporter statusReporter;
    private Logger logger;

    private List<String[]> writtenRecords;

    @Before
    public void setUpMocks() throws IOException {
        autodetectProcess = Mockito.mock(AutodetectProcess.class);
        statusReporter = Mockito.mock(StatusReporter.class);
        logger = Mockito.mock(Logger.class);

        writtenRecords = new ArrayList<>();
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                String[] record = (String[]) invocation.getArguments()[0];
                String[] copy = Arrays.copyOf(record, record.length);
                writtenRecords.add(copy);
                return null;
            }
        }).when(autodetectProcess).writeRecord(any(String[].class));
    }

    public void testCsvWriteWithConcat() throws IOException {
        StringBuilder input = new StringBuilder();
        input.append("time,host,metric,value\n");
        input.append("1,hostA,foo,3.0\n");
        input.append("2,hostB,bar,2.0\n");
        input.append("2,hostA,bar,2.0\n");

        InputStream inputStream = createInputStream(input.toString());
        AbstractDataToProcessWriter writer = createWriter(true);
        writer.writeHeader();
        writer.write(inputStream);

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "concat", "value", "."});
        expectedRecords.add(new String[]{"1", "hostAfoo", "3.0", ""});
        expectedRecords.add(new String[]{"2", "hostBbar", "2.0", ""});
        expectedRecords.add(new String[]{"2", "hostAbar", "2.0", ""});

        assertWrittenRecordsEqualTo(expectedRecords);

        verify(statusReporter).finishReporting();
    }

    public void testJsonWriteWithConcat() throws IOException {
        StringBuilder input = new StringBuilder();
        input.append("{\"time\" : 1, \"host\" : \"hostA\", \"metric\" : \"foo\", \"value\" : 3.0}\n");
        input.append("{\"time\" : 2, \"host\" : \"hostB\", \"metric\" : \"bar\", \"value\" : 2.0}\n");
        input.append("{\"time\" : 2, \"host\" : \"hostA\", \"metric\" : \"bar\", \"value\" : 2.0}\n");

        InputStream inputStream = createInputStream(input.toString());
        AbstractDataToProcessWriter writer = createWriter(false);
        writer.writeHeader();
        writer.write(inputStream);

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "concat", "value", "."});
        expectedRecords.add(new String[]{"1", "hostAfoo", "3.0", ""});
        expectedRecords.add(new String[]{"2", "hostBbar", "2.0", ""});
        expectedRecords.add(new String[]{"2", "hostAbar", "2.0", ""});

        assertWrittenRecordsEqualTo(expectedRecords);

        verify(statusReporter).finishReporting();
    }


    private static InputStream createInputStream(String input) {
        return new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    }

    private AbstractDataToProcessWriter createWriter(boolean doCsv) {
        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setFieldDelimiter(',');
        dd.setFormat(doCsv ? DataFormat.DELIMITED : DataFormat.JSON);
        dd.setTimeFormat(DataDescription.EPOCH);

        Detector.Builder detector = new Detector.Builder("metric", "value");
        detector.setByFieldName("concat");
        AnalysisConfig ac = new AnalysisConfig.Builder(Arrays.asList(detector.build())).build();

        TransformConfig tc = new TransformConfig(TransformType.Names.CONCAT_NAME);
        tc.setInputs(Arrays.asList("host", "metric"));

        TransformConfigs tcs = new TransformConfigs(Arrays.asList(tc));

        if (doCsv) {
            return new CsvDataToProcessWriter(true, autodetectProcess, dd.build(), ac, tcs, statusReporter, logger);
        } else {
            return new JsonDataToProcessWriter(true, autodetectProcess, dd.build(), ac, tcs, statusReporter, logger);
        }
    }

    private void assertWrittenRecordsEqualTo(List<String[]> expectedRecords) {
        assertEquals(expectedRecords.size(), writtenRecords.size());
        for (int i = 0; i < expectedRecords.size(); i++) {
            assertArrayEquals(expectedRecords.get(i), writtenRecords.get(i));
        }
    }
}
