/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.analysis.AnalysisRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.core.ml.job.config.CategorizationAnalyzerConfig;
import org.elasticsearch.xpack.core.ml.job.config.DataDescription;
import org.elasticsearch.xpack.core.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzer;
import org.elasticsearch.xpack.ml.job.categorization.CategorizationAnalyzerTests;
import org.elasticsearch.xpack.ml.job.process.DataCountsReporter;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcess;
import org.elasticsearch.xpack.ml.job.process.autodetect.writer.AbstractDataToProcessWriter.InputOutputMap;
import org.elasticsearch.xpack.ml.process.writer.LengthEncodedWriter;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AbstractDataToProcessWriterTests extends ESTestCase {

    private AnalysisRegistry analysisRegistry;
    private Environment environment;
    private AutodetectProcess autodetectProcess;
    private DataCountsReporter dataCountsReporter;

    @Before
    public void setup() throws Exception {
        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        environment = TestEnvironment.newEnvironment(settings);
        analysisRegistry = CategorizationAnalyzerTests.buildTestAnalysisRegistry(environment);
        autodetectProcess = Mockito.mock(AutodetectProcess.class);
        dataCountsReporter = Mockito.mock(DataCountsReporter.class);
    }

    /**
     * Testing methods of AbstractDataToProcessWriter but uses the concrete instances.
     */
    public void testInputFields() throws IOException {
        DataDescription.Builder dd = new DataDescription.Builder();
        dd.setTimeField("time_field");

        Detector.Builder detector = new Detector.Builder("metric", "value");
        detector.setByFieldName("metric");
        detector.setPartitionFieldName("host");
        detector.setDetectorDescription("metric(value) by metric partitionfield=host");
        AnalysisConfig ac = new AnalysisConfig.Builder(Collections.singletonList(detector.build())).build();

        boolean includeTokensFields = randomBoolean();
        AbstractDataToProcessWriter writer = new CsvDataToProcessWriter(
            true,
            includeTokensFields,
            autodetectProcess,
            dd.build(),
            ac,
            dataCountsReporter
        );

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
        assertEquals(Integer.valueOf(0), inputIndexes.get("time_field"));
        assertEquals(Integer.valueOf(1), inputIndexes.get("metric"));
        assertEquals(Integer.valueOf(2), inputIndexes.get("host"));
        assertEquals(Integer.valueOf(3), inputIndexes.get("value"));

        Map<String, Integer> outputIndexes = writer.outputFieldIndexes();
        assertEquals(includeTokensFields ? 6 : 5, outputIndexes.size());
        assertEquals(Integer.valueOf(0), outputIndexes.get("time_field"));
        assertEquals(Integer.valueOf(1), outputIndexes.get("host"));
        assertEquals(Integer.valueOf(2), outputIndexes.get("metric"));
        assertEquals(Integer.valueOf(3), outputIndexes.get("value"));
        if (includeTokensFields) {
            assertEquals(Integer.valueOf(4), outputIndexes.get(LengthEncodedWriter.PRETOKENISED_TOKEN_FIELD));
            assertEquals(Integer.valueOf(5), outputIndexes.get(LengthEncodedWriter.CONTROL_FIELD_NAME));
        } else {
            assertEquals(Integer.valueOf(4), outputIndexes.get(LengthEncodedWriter.CONTROL_FIELD_NAME));
        }

        List<InputOutputMap> inOutMaps = writer.getInputOutputMap();
        assertEquals(4, inOutMaps.size());
        assertEquals(0, inOutMaps.get(0).inputIndex);
        assertEquals(0, inOutMaps.get(0).outputIndex);
        assertEquals(2, inOutMaps.get(1).inputIndex);
        assertEquals(1, inOutMaps.get(1).outputIndex);
        assertEquals(1, inOutMaps.get(2).inputIndex);
        assertEquals(2, inOutMaps.get(2).outputIndex);
        assertEquals(3, inOutMaps.get(3).inputIndex);
        assertEquals(3, inOutMaps.get(3).outputIndex);
    }

    public void testTokenizeForCategorization() throws IOException {
        CategorizationAnalyzerConfig defaultConfig = CategorizationAnalyzerConfig.buildDefaultCategorizationAnalyzer(null);
        try (CategorizationAnalyzer categorizationAnalyzer = new CategorizationAnalyzer(analysisRegistry, defaultConfig)) {

            assertEquals(
                "sol13m-8608.1.p2ps,Info,Source,AES_SERVICE2,on,has,shut,down",
                AbstractDataToProcessWriter.tokenizeForCategorization(
                    categorizationAnalyzer,
                    "p2ps",
                    "<sol13m-8608.1.p2ps: Info: > Source AES_SERVICE2 on 33122:967 has shut down."
                )
            );

            assertEquals(
                "Vpxa,verbose,VpxaHalCnxHostagent,opID,WFU-ddeadb59,WaitForUpdatesDone,Received,callback",
                AbstractDataToProcessWriter.tokenizeForCategorization(
                    categorizationAnalyzer,
                    "vmware",
                    "Vpxa: [49EC0B90 verbose 'VpxaHalCnxHostagent' opID=WFU-ddeadb59] [WaitForUpdatesDone] Received callback"
                )
            );

            assertEquals(
                "org.apache.coyote.http11.Http11BaseProtocol,destroy",
                AbstractDataToProcessWriter.tokenizeForCategorization(
                    categorizationAnalyzer,
                    "apache",
                    "org.apache.coyote.http11.Http11BaseProtocol destroy"
                )
            );

            assertEquals(
                "INFO,session,PROXY,Session,DESTROYED",
                AbstractDataToProcessWriter.tokenizeForCategorization(
                    categorizationAnalyzer,
                    "proxy",
                    " [1111529792] INFO  session <45409105041220090733@62.218.251.123> - "
                        + "----------------- PROXY Session DESTROYED --------------------"
                )
            );

            assertEquals(
                "PSYoungGen,total,used",
                AbstractDataToProcessWriter.tokenizeForCategorization(
                    categorizationAnalyzer,
                    "java",
                    "PSYoungGen      total 2572800K, used 1759355K [0x0000000759500000, 0x0000000800000000, 0x0000000800000000)"
                )
            );
        }
    }

    public void testMaybeTruncateCategorizationField() {
        {
            DataDescription.Builder dd = new DataDescription.Builder();
            dd.setTimeField("time_field");

            Detector.Builder detector = new Detector.Builder("count", "");
            detector.setByFieldName("mlcategory");
            AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
            builder.setCategorizationFieldName("message");
            AnalysisConfig ac = builder.build();

            boolean includeTokensFields = randomBoolean();
            AbstractDataToProcessWriter writer = new JsonDataToProcessWriter(
                true,
                includeTokensFields,
                autodetectProcess,
                dd.build(),
                ac,
                dataCountsReporter,
                NamedXContentRegistry.EMPTY
            );

            String truncatedField = writer.maybeTruncateCatgeorizationField(randomAlphaOfLengthBetween(1002, 2000));
            assertEquals(AnalysisConfig.MAX_CATEGORIZATION_FIELD_LENGTH, truncatedField.length());
        }
        {
            DataDescription.Builder dd = new DataDescription.Builder();
            dd.setTimeField("time_field");

            Detector.Builder detector = new Detector.Builder("count", "");
            detector.setByFieldName("mlcategory");
            AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
            builder.setCategorizationFieldName("message");
            AnalysisConfig ac = builder.build();

            boolean includeTokensFields = randomBoolean();
            AbstractDataToProcessWriter writer = new JsonDataToProcessWriter(
                true,
                includeTokensFields,
                autodetectProcess,
                dd.build(),
                ac,
                dataCountsReporter,
                NamedXContentRegistry.EMPTY
            );

            String categorizationField = randomAlphaOfLengthBetween(1, 1000);
            String truncatedField = writer.maybeTruncateCatgeorizationField(categorizationField);
            assertEquals(categorizationField.length(), truncatedField.length());
        }
        {
            DataDescription.Builder dd = new DataDescription.Builder();
            dd.setTimeField("time_field");

            Detector.Builder detector = new Detector.Builder("count", "");
            detector.setByFieldName("mlcategory");
            detector.setPartitionFieldName("message");
            AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Collections.singletonList(detector.build()));
            builder.setCategorizationFieldName("message");
            AnalysisConfig ac = builder.build();

            boolean includeTokensFields = randomBoolean();
            AbstractDataToProcessWriter writer = new JsonDataToProcessWriter(
                true,
                includeTokensFields,
                autodetectProcess,
                dd.build(),
                ac,
                dataCountsReporter,
                NamedXContentRegistry.EMPTY
            );

            String truncatedField = writer.maybeTruncateCatgeorizationField(randomAlphaOfLengthBetween(1002, 2000));
            assertFalse(AnalysisConfig.MAX_CATEGORIZATION_FIELD_LENGTH == truncatedField.length());
        }
    }
}
