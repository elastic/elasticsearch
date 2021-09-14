/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentGenerator;
import org.elasticsearch.common.xcontent.XContentType;
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
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class JsonDataToProcessWriterTests extends ESTestCase {

    private AnalysisRegistry analysisRegistry;
    private Environment environment;

    private AutodetectProcess autodetectProcess;
    private DataCountsReporter dataCountsReporter;

    private DataDescription.Builder dataDescription;
    private AnalysisConfig analysisConfig;

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
        dataDescription.setTimeFormat(DataDescription.EPOCH);

        Detector detector = new Detector.Builder("metric", "value").build();
        analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(detector))
            .setBucketSpan(TimeValue.timeValueSeconds(1))
            .build();
    }

    public void testWrite_GivenTimeFormatIsEpochAndDataIsValid() throws Exception {
        StringBuilder input = new StringBuilder();
        input.append("{\"time\":\"1\", \"metric\":\"foo\", \"value\":\"1.0\"}");
        input.append("{\"time\":\"2\", \"metric\":\"bar\", \"value\":\"2.0\"}");
        InputStream inputStream = createInputStream(input.toString());
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, XContentType.JSON, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "value", "."});
        expectedRecords.add(new String[]{"1", "1.0", ""});
        expectedRecords.add(new String[]{"2", "2.0", ""});
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter).finishReporting();
    }

    public void testWrite_GivenTimeFormatIsEpochAndCategorization() throws Exception {
        Detector.Builder detector = new Detector.Builder("count", null);
        detector.setByFieldName("mlcategory");
        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
        builder.setCategorizationFieldName("message");
        builder.setCategorizationAnalyzerConfig(CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(null));
        analysisConfig = builder.build();

        StringBuilder input = new StringBuilder();
        input.append("{\"time\":\"1\", \"message\":\"Node 1 started\"}");
        input.append("{\"time\":\"2\", \"message\":\"Node 2 started\"}");
        InputStream inputStream = createInputStream(input.toString());
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        try (CategorizationAnalyzer categorizationAnalyzer =
                     new CategorizationAnalyzer(analysisRegistry, analysisConfig.getCategorizationAnalyzerConfig())) {
            writer.write(inputStream, categorizationAnalyzer, XContentType.JSON, (r, e) -> {});
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

        verify(dataCountsReporter).finishReporting();
    }

    public void testWrite_GivenTimeFormatIsEpochAndTimestampsAreOutOfOrder() throws Exception {
        StringBuilder input = new StringBuilder();
        input.append("{\"time\":\"3\", \"metric\":\"foo\", \"value\":\"3.0\"}");
        input.append("{\"time\":\"1\", \"metric\":\"bar\", \"value\":\"1.0\"}");
        input.append("{\"time\":\"2\", \"metric\":\"bar\", \"value\":\"2.0\"}");
        InputStream inputStream = createInputStream(input.toString());
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, XContentType.JSON, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "value", "."});
        expectedRecords.add(new String[]{"3", "3.0", ""});
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter, times(2)).reportOutOfOrderRecord(2);
        verify(dataCountsReporter, never()).reportLatestTimeIncrementalStats(anyLong());
        verify(dataCountsReporter).finishReporting();
    }

    public void testWrite_GivenTimeFormatIsEpochAndSomeTimestampsOutOfOrderWithinBucketSpan() throws Exception {
        analysisConfig = new AnalysisConfig.Builder(
            Collections.singletonList(
                new Detector.Builder("metric", "value").build()
            ))
            .setBucketSpan(TimeValue.timeValueSeconds(10))
            .build();

        StringBuilder input = new StringBuilder();
        input.append("{\"time\":\"4\", \"metric\":\"foo\", \"value\":\"4.0\"}");
        input.append("{\"time\":\"5\", \"metric\":\"foo\", \"value\":\"5.0\"}");
        input.append("{\"time\":\"3\", \"metric\":\"bar\", \"value\":\"3.0\"}");
        input.append("{\"time\":\"4\", \"metric\":\"bar\", \"value\":\"4.0\"}");
        input.append("{\"time\":\"2\", \"metric\":\"bar\", \"value\":\"2.0\"}");
        input.append("{\"time\":\"12\", \"metric\":\"bar\", \"value\":\"12.0\"}");
        input.append("{\"time\":\"2\", \"metric\":\"bar\", \"value\":\"2.0\"}");
        InputStream inputStream = createInputStream(input.toString());
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, XContentType.JSON, (r, e) -> {});

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "value", "."});
        expectedRecords.add(new String[]{"4", "4.0", ""});
        expectedRecords.add(new String[]{"5", "5.0", ""});
        expectedRecords.add(new String[]{"3", "3.0", ""});
        expectedRecords.add(new String[]{"4", "4.0", ""});
        expectedRecords.add(new String[]{"2", "2.0", ""});
        expectedRecords.add(new String[]{"12", "12.0", ""});
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter, times(1)).reportOutOfOrderRecord(2);
        verify(dataCountsReporter, never()).reportLatestTimeIncrementalStats(anyLong());
        verify(dataCountsReporter).finishReporting();
    }

    public void testWrite_GivenTimeFormatIsEpochAndSomeTimestampsWithinLatencySomeOutOfOrder() throws Exception {
        analysisConfig = new AnalysisConfig.Builder(
            Collections.singletonList(
                new Detector.Builder("metric", "value").build()
            ))
            .setLatency(TimeValue.timeValueSeconds(2))
            .setBucketSpan(TimeValue.timeValueSeconds(1)).setLatency(TimeValue.timeValueSeconds(2))
            .build();

        StringBuilder input = new StringBuilder();
        input.append("{\"time\":\"4\", \"metric\":\"foo\", \"value\":\"4.0\"}");
        input.append("{\"time\":\"5\", \"metric\":\"foo\", \"value\":\"5.0\"}");
        input.append("{\"time\":\"3\", \"metric\":\"bar\", \"value\":\"3.0\"}");
        input.append("{\"time\":\"4\", \"metric\":\"bar\", \"value\":\"4.0\"}");
        input.append("{\"time\":\"2\", \"metric\":\"bar\", \"value\":\"2.0\"}");
        InputStream inputStream = createInputStream(input.toString());
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, XContentType.JSON, (r, e) -> {});

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "value", "."});
        expectedRecords.add(new String[]{"4", "4.0", ""});
        expectedRecords.add(new String[]{"5", "5.0", ""});
        expectedRecords.add(new String[]{"3", "3.0", ""});
        expectedRecords.add(new String[]{"4", "4.0", ""});
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter, times(1)).reportOutOfOrderRecord(2);
        verify(dataCountsReporter, never()).reportLatestTimeIncrementalStats(anyLong());
        verify(dataCountsReporter).finishReporting();
    }

    public void testWrite_GivenMalformedJsonWithoutNestedLevels() throws Exception {
        AnalysisConfig.Builder builder =
                new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("metric", "value").build()));
        builder.setLatency(TimeValue.timeValueSeconds(2));
        analysisConfig = builder.build();

        StringBuilder input = new StringBuilder();
        input.append("{\"time\":\"1\", \"value\":\"1.0\"}");
        input.append("{\"time\":\"2\" \"value\":\"2.0\"}");
        input.append("{\"time\":\"3\", \"value\":\"3.0\"}");
        InputStream inputStream = createInputStream(input.toString());
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, XContentType.JSON, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "value", "."});
        expectedRecords.add(new String[]{"1", "1.0", ""});
        expectedRecords.add(new String[]{"2", "", ""});
        expectedRecords.add(new String[]{"3", "3.0", ""});
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter).reportMissingFields(1);
        verify(dataCountsReporter).finishReporting();
    }

    public void testWrite_GivenMalformedJsonWithNestedLevels()
            throws Exception {
        Detector detector = new Detector.Builder("metric", "nested.value").build();
        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Collections.singletonList(detector));
        builder.setLatency(TimeValue.timeValueSeconds(2));
        analysisConfig = builder.build();

        StringBuilder input = new StringBuilder();
        input.append("{\"time\":\"1\", \"nested\":{\"value\":\"1.0\"}}");
        input.append("{\"time\":\"2\", \"nested\":{\"value\":\"2.0\"} \"foo\":\"bar\"}");
        input.append("{\"time\":\"3\", \"nested\":{\"value\":\"3.0\"}}");
        InputStream inputStream = createInputStream(input.toString());
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, XContentType.JSON, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "nested.value", "."});
        expectedRecords.add(new String[]{"1", "1.0", ""});
        expectedRecords.add(new String[]{"2", "2.0", ""});
        expectedRecords.add(new String[]{"3", "3.0", ""});
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter).finishReporting();
    }

    public void testWrite_GivenMalformedJsonThatNeverRecovers()
            throws Exception {
        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("count", null).build()));
        builder.setLatency(TimeValue.timeValueSeconds(2));
        analysisConfig = builder.build();

        StringBuilder input = new StringBuilder();
        input.append("{\"time\":\"1\", \"value\":\"2.0\"}");
        input.append("{\"time");
        InputStream inputStream = createInputStream(input.toString());
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();

        ESTestCase.expectThrows(ElasticsearchParseException.class,
                () -> writer.write(inputStream, null, XContentType.JSON, (r, e) -> {}));
    }

    public void testWrite_GivenJsonWithArrayField() throws Exception {
        AnalysisConfig.Builder builder =
                new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("metric", "value").build()));
        builder.setLatency(TimeValue.timeValueSeconds(2));
        analysisConfig = builder.build();

        StringBuilder input = new StringBuilder();
        input.append("{\"time\":\"1\", \"array\":[\"foo\", \"bar\"], \"value\":\"1.0\"}");
        input.append("{\"time\":\"2\", \"array\":[], \"value\":\"2.0\"}");
        InputStream inputStream = createInputStream(input.toString());
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, XContentType.JSON, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "value", "."});
        expectedRecords.add(new String[]{"1", "1.0", ""});
        expectedRecords.add(new String[]{"2", "2.0", ""});
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter).finishReporting();
    }

    public void testWrite_GivenJsonWithMissingFields() throws Exception {
        AnalysisConfig.Builder builder =
                new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("metric", "value").build()));
        builder.setLatency(TimeValue.timeValueSeconds(2));
        analysisConfig = builder.build();

        StringBuilder input = new StringBuilder();
        input.append("{\"time\":\"1\", \"f1\":\"foo\", \"value\":\"1.0\"}");
        input.append("{\"time\":\"2\", \"value\":\"2.0\"}");
        input.append("{\"time\":\"3\", \"f1\":\"bar\"}");
        input.append("{}");
        input.append("{\"time\":\"4\", \"value\":\"3.0\"}");

        InputStream inputStream = createInputStream(input.toString());
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, XContentType.JSON, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "value", "."});
        expectedRecords.add(new String[]{"1", "1.0", ""});
        expectedRecords.add(new String[]{"2", "2.0", ""});
        expectedRecords.add(new String[]{"3", "", ""});
        expectedRecords.add(new String[]{"4", "3.0", ""});
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter, times(1)).reportMissingFields(1L);
        verify(dataCountsReporter, times(1)).reportRecordWritten(2, 1000, 1000);
        verify(dataCountsReporter, times(1)).reportRecordWritten(1, 2000, 2000);
        verify(dataCountsReporter, times(1)).reportRecordWritten(1, 3000, 3000);
        verify(dataCountsReporter, times(1)).reportRecordWritten(1, 4000, 4000);
        verify(dataCountsReporter, times(1)).reportDateParseError(0);
        verify(dataCountsReporter).finishReporting();
    }

    public void testWrite_Smile() throws Exception {

        BytesStreamOutput xsonOs = new BytesStreamOutput();
        XContentGenerator xsonGen = XContentFactory.xContent(XContentType.SMILE).createGenerator(xsonOs);
        xsonGen.writeStartObject();
        xsonGen.writeStringField("time", "1");
        xsonGen.writeStringField("metric", "foo");
        xsonGen.writeStringField("value", "1.0");
        xsonGen.writeEndObject();
        xsonGen.close();
        xsonOs.writeByte(XContentType.SMILE.xContent().streamSeparator());

        xsonGen = XContentFactory.xContent(XContentType.SMILE).createGenerator(xsonOs);
        xsonGen.writeStartObject();
        xsonGen.writeStringField("time", "2");
        xsonGen.writeStringField("metric", "bar");
        xsonGen.writeStringField("value", "2.0");
        xsonGen.writeEndObject();
        xsonGen.flush();
        xsonOs.writeByte(XContentType.SMILE.xContent().streamSeparator());

        InputStream inputStream = new ByteArrayInputStream(BytesReference.toBytes(xsonOs.bytes()));
        JsonDataToProcessWriter writer = createWriter();
        writer.writeHeader();
        writer.write(inputStream, null, XContentType.SMILE, (r, e) -> {});
        verify(dataCountsReporter, times(1)).startNewIncrementalCount();

        List<String[]> expectedRecords = new ArrayList<>();
        // The final field is the control field
        expectedRecords.add(new String[]{"time", "value", "."});
        expectedRecords.add(new String[]{"1", "1.0", ""});
        expectedRecords.add(new String[]{"2", "2.0", ""});
        assertWrittenRecordsEqualTo(expectedRecords);

        verify(dataCountsReporter).finishReporting();
    }

    private static InputStream createInputStream(String input) {
        return new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
    }

    private JsonDataToProcessWriter createWriter() {
        boolean includeTokensField = MachineLearning.CATEGORIZATION_TOKENIZATION_IN_JAVA &&
                analysisConfig.getCategorizationFieldName() != null;
        return new JsonDataToProcessWriter(true, includeTokensField, autodetectProcess, dataDescription.build(), analysisConfig,
                dataCountsReporter, new NamedXContentRegistry(Collections.emptyList()));
    }

    private void assertWrittenRecordsEqualTo(List<String[]> expectedRecords) {
        for (int i = 0; i < expectedRecords.size(); i++) {
            assertArrayEquals(expectedRecords.get(i), writtenRecords.get(i));
        }
    }

}
