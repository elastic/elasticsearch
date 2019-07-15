/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzerTests;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription.DataFormat;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.supercsv.exception.SuperCsvException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CsvDataToProcessWriterTests extends ESTestCase {

    private AnalysisRegistry analysisRegistry;
    private Environment environment;

    private AutodetectProcess autodetectProcess;
    private DataDescription.Builder dataDescription;
    private AnalysisConfig analysisConfig;
    private DataCountsReporter dataCountsReporter;

    private List<String[]> writtenRecords;

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        environment = TestEnvironment.newEnvironment(settings);
        analysisRegistry = CategorizationAnalyzerTests.buildTestAnalysisRegistry(environment);

        autodetectProcess = Mockito.mock(AutodetectProcess.class);
        dataCountsReporter = Mockito.mock(DataCountsReporter.class);

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

        dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataFormat.DELIMITED);
        dataDescription.setFieldDelimiter(',');
        dataDescription.setTimeFormat(DataDescription.EPOCH);

        Detector detector = new Detector.Builder("metric", "value").build();
        analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector)).build();
    }

    public void testWrite_GivenTimeFormatIsEpochAndDataIsValid() throws IOException {
        StringBuilder input = new StringBuilder();
        input.append("time,metric,value\n");
        input.append("1,foo,1.0\n");
        input.append("2,bar,2.0\n");
        InputStream inputStream = createInputStream(input.toString());
        CsvDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, null, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[] { "time", "value", "." });
        expectedRecords.add(new String[] { "1", "1.0", "" });
        expectedRecords.add(new String[] { "2", "2.0", "" });
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter).finishReporting(any());
    }

    public void testWrite_GivenTimeFormatIsEpochAndCategorization() throws IOException {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("mlcategory");
        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        builder.setCategorizationFieldName("message");
        builder.setCategorizationAnalyzerConfig(CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(null));
        analysisConfig = builder.build();

        StringBuilder input = new StringBuilder();
        input.append("time,message\n");
        input.append("1,Node 1 started\n");
        input.append("2,Node 2 started\n");
        InputStream inputStream = createInputStream(input.toString());
        CsvDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        try (CategorizationAnalyzer categorizationAnalyzer =
                     new CategorizationAnalyzer(analysisRegistry, analysisConfig.getCategorizationAnalyzerConfig())) {
            writer.write(inputStream, categorizationAnalyzer, null, (r, e) -> {});
        }
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The "." field is the control field; "..." is the pre-tokenized tokens field
        if (MachineLearning.CATEGORIZATION_TOKENIZATION_IN_JAVA) {
            expectedRecords.add(new String[]{"time", "message", "...", "."});
            expectedRecords.add(new String[]{"1", "Node 1 started", "Node,started", ""});
            expectedRecords.add(new String[]{"2", "Node 2 started", "Node,started", ""});
        } else {
            expectedRecords.add(new String[]{"time", "message", "."});
            expectedRecords.add(new String[]{"1", "Node 1 started", ""});
            expectedRecords.add(new String[]{"2", "Node 2 started", ""});
        }
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter).finishReporting(any());
    }

    public void testWrite_GivenTimeFormatIsEpochAndTimestampsAreOutOfOrder() throws IOException {
        StringBuilder input = new StringBuilder();
        input.append("time,metric,value\n");
        input.append("3,foo,3.0\n");
        input.append("1,bar,2.0\n");
        input.append("2,bar,2.0\n");
        InputStream inputStream = createInputStream(input.toString());
        CsvDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, null, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[] { "time", "value", "." });
        expectedRecords.add(new String[] { "3", "3.0", "" });
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter, times(2)).reportOutOfOrderRecord(2);
        verify(dataCountsReporter, never()).reportLatestTimeIncrementalStats(anyLong());
        verify(dataCountsReporter).finishReporting(any());
    }

    public void testWrite_GivenTimeFormatIsEpochAndAllRecordsAreOutOfOrder() throws IOException {
        StringBuilder input = new StringBuilder();
        input.append("time,metric,value\n");
        input.append("1,foo,1.0\n");
        input.append("2,bar,2.0\n");
        InputStream inputStream = createInputStream(input.toString());

        when(dataCountsReporter.getLatestRecordTime()).thenReturn(new Date(5000L));
        CsvDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, null, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[] { "time", "value", "." });
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter, times(2)).reportOutOfOrderRecord(2);
        verify(dataCountsReporter, times(2)).reportLatestTimeIncrementalStats(anyLong());
        verify(dataCountsReporter, never()).reportRecordWritten(anyLong(), anyLong());
        verify(dataCountsReporter).finishReporting(any());
    }

    public void testWrite_GivenTimeFormatIsEpochAndSomeTimestampsWithinLatencySomeOutOfOrder() throws IOException {
        AnalysisConfig.Builder builder =
                new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("metric", "value").build()));
        builder.setLatency(TimeValue.timeValueSeconds(2));
        analysisConfig = builder.build();

        StringBuilder input = new StringBuilder();
        input.append("time,metric,value\n");
        input.append("4,foo,4.0\n");
        input.append("5,foo,5.0\n");
        input.append("3,foo,3.0\n");
        input.append("4,bar,4.0\n");
        input.append("2,bar,2.0\n");
        input.append("\0");
        InputStream inputStream = createInputStream(input.toString());
        CsvDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, null, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[] { "time", "value", "." });
        expectedRecords.add(new String[] { "4", "4.0", "" });
        expectedRecords.add(new String[] { "5", "5.0", "" });
        expectedRecords.add(new String[] { "3", "3.0", "" });
        expectedRecords.add(new String[] { "4", "4.0", "" });
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter, times(1)).reportOutOfOrderRecord(2);
        verify(dataCountsReporter, never()).reportLatestTimeIncrementalStats(anyLong());
        verify(dataCountsReporter).finishReporting(any());
    }

    public void testWrite_NullByte() throws IOException {
        AnalysisConfig.Builder builder =
                new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("metric", "value").build()));
        builder.setLatency(TimeValue.ZERO);
        analysisConfig = builder.build();

        StringBuilder input = new StringBuilder();
        input.append("metric,value,time\n");
        input.append("foo,4.0,1\n");
        input.append("\0"); // the csv reader treats this as a line (even though it doesn't end with \n) and skips over it
        input.append("foo,5.0,2\n");
        input.append("foo,3.0,3\n");
        input.append("bar,4.0,4\n");
        input.append("\0");
        InputStream inputStream = createInputStream(input.toString());
        CsvDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, null, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[] { "time", "value", "." });
        expectedRecords.add(new String[] { "1", "4.0", "" });
        expectedRecords.add(new String[] { "2", "5.0", "" });
        expectedRecords.add(new String[] { "3", "3.0", "" });
        expectedRecords.add(new String[] { "4", "4.0", "" });
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter, times(2)).reportMissingField();
        verify(dataCountsReporter, times(1)).reportRecordWritten(2, 1000);
        verify(dataCountsReporter, times(1)).reportRecordWritten(2, 2000);
        verify(dataCountsReporter, times(1)).reportRecordWritten(2, 3000);
        verify(dataCountsReporter, times(1)).reportRecordWritten(2, 4000);
        verify(dataCountsReporter, times(1)).reportDateParseError(2);
        verify(dataCountsReporter).finishReporting(any());
    }

    @SuppressWarnings("unchecked")
    public void testWrite_EmptyInput() throws IOException {
        AnalysisConfig.Builder builder =
                new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("metric", "value").build()));
        builder.setLatency(TimeValue.ZERO);
        analysisConfig = builder.build();

        when(dataCountsReporter.incrementalStats()).thenReturn(new DataCounts("foo"));

        doAnswer(invocation -> {
            ActionListener<Boolean> listener = (ActionListener<Boolean>) invocation.getArguments()[0];
            listener.onResponse(true);
            return null;
        }).when(dataCountsReporter).finishReporting(any());

        InputStream inputStream = createInputStream("");
        CsvDataToProcessWriter writer = createWriter();
        writer.writeHeader();

        writer.write(inputStream, null, null, (counts, e) -> {
            if (e != null) {
                fail(e.getMessage());
            } else {
                assertEquals(0L, counts.getInputBytes());
                assertEquals(0L, counts.getInputRecordCount());
            }
        });
    }

    public void testWrite_GivenMisplacedQuoteMakesRecordExtendOverTooManyLines() throws IOException {

        StringBuilder input = new StringBuilder();
        input.append("time,metric,value\n");
        input.append("1,\"foo,1.0\n");
        for (int i = 0; i < 10000 - 1; i++) {
            input.append("\n");
        }
        input.append("2,bar\",2.0\n");
        InputStream inputStream = createInputStream(input.toString());
        CsvDataToProcessWriter writer = createWriter();
        writer.writeHeader();

        SuperCsvException e = ESTestCase.expectThrows(SuperCsvException.class,
                () -> writer.write(inputStream, null, null, (response, error) -> {}));
        // Expected line numbers are 2 and 10001, but SuperCSV may print the
        // numbers using a different locale's digit characters
        assertTrue(e.getMessage(), e.getMessage().matches(
                "max number of lines to read exceeded while reading quoted column beginning on line . and ending on line ....."));
    }

    private static InputStream createInputStream(String input) {
        return new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    }

    private CsvDataToProcessWriter createWriter() {
        boolean includeTokensField = MachineLearning.CATEGORIZATION_TOKENIZATION_IN_JAVA &&
                analysisConfig.getCategorizationFieldName() != null;
        return new CsvDataToProcessWriter(true, includeTokensField, autodetectProcess, dataDescription.build(), analysisConfig,
                dataCountsReporter);
    }

    private void assertWrittenRecordsEqualTo(List<String[]> expectedRecords) {
        assertEquals(expectedRecords.size(), writtenRecords.size());
        for (int i = 0; i < expectedRecords.size(); i++) {
            assertArrayEquals(expectedRecords.get(i), writtenRecords.get(i));
        }
    }
}
