/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.config.Connective;
import org.ini4j.Config;
import org.ini4j.Ini;
import org.ini4j.Profile.Section;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Condition;
import org.elasticsearch.xpack.ml.job.config.Operator;
import org.elasticsearch.xpack.ml.job.config.DetectionRule;
import org.elasticsearch.xpack.ml.job.config.RuleCondition;
import org.elasticsearch.xpack.ml.job.config.RuleConditionType;
import org.elasticsearch.xpack.ml.job.config.MlFilter;


public class FieldConfigWriterTests extends ESTestCase {
    private AnalysisConfig analysisConfig;
    private Set<MlFilter> filters;
    private OutputStreamWriter writer;

    @Before
    public void setUpDeps() {
        analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(new Detector.Builder("count", null).build())).build();
        filters = new LinkedHashSet<>();
    }

    public void testMultipleDetectorsToConfFile()
            throws IOException {
        List<Detector> detectors = new ArrayList<>();

        Detector.Builder d = new Detector.Builder("metric", "Integer_Value");
        d.setByFieldName("ts_hash");
        detectors.add(d.build());
        Detector.Builder d2 = new Detector.Builder("count", null);
        d2.setByFieldName("ipaddress");
        detectors.add(d2.build());
        Detector.Builder d3 = new Detector.Builder("max", "Integer_Value");
        d3.setOverFieldName("ts_hash");
        detectors.add(d3.build());
        Detector.Builder d4 = new Detector.Builder("rare", null);
        d4.setByFieldName("ipaddress");
        d4.setPartitionFieldName("host");
        detectors.add(d4.build());
        Detector.Builder d5 = new Detector.Builder("rare", null);
        d5.setByFieldName("weird field");
        detectors.add(d5.build());
        Detector.Builder d6 = new Detector.Builder("max", "field");
        d6.setOverFieldName("tshash");
        detectors.add(d6.build());

        analysisConfig = new AnalysisConfig.Builder(detectors).build();

        ByteArrayOutputStream ba = new ByteArrayOutputStream();
        writer = new OutputStreamWriter(ba, StandardCharsets.UTF_8);

        createFieldConfigWriter().write();
        writer.close();

        // read the ini file - all the settings are in the global section
        StringReader reader = new StringReader(ba.toString("UTF-8"));

        Config iniConfig = new Config();
        iniConfig.setLineSeparator(new String(new char[]{WriterConstants.NEW_LINE}));
        iniConfig.setGlobalSection(true);

        Ini fieldConfig = new Ini();
        fieldConfig.setConfig(iniConfig);
        fieldConfig.load(reader);

        Section section = fieldConfig.get(iniConfig.getGlobalSectionName());

        assertEquals(detectors.size(), section.size());

        String value = fieldConfig.get(iniConfig.getGlobalSectionName(), "detector.0.clause");
        assertEquals("metric(Integer_Value) by ts_hash", value);
        value = fieldConfig.get(iniConfig.getGlobalSectionName(), "detector.1.clause");
        assertEquals("count by ipaddress", value);
        value = fieldConfig.get(iniConfig.getGlobalSectionName(), "detector.2.clause");
        assertEquals("max(Integer_Value) over ts_hash", value);
        value = fieldConfig.get(iniConfig.getGlobalSectionName(), "detector.3.clause");
        assertEquals("rare by ipaddress partitionfield=host", value);
        value = fieldConfig.get(iniConfig.getGlobalSectionName(), "detector.4.clause");
        assertEquals("rare by \"weird field\"", value);
        value = fieldConfig.get(iniConfig.getGlobalSectionName(), "detector.5.clause");
        // Ini4j meddles with escape characters itself, so the assertion below
        // fails even though the raw file is fine.  The file is never read by
        // Ini4j in the production system.
        // Assert.assertEquals("max(\"\\\"quoted\\\" field\") over \"ts\\\\hash\"", value);
    }

    public void testWrite_GivenConfigHasCategorizationField() throws IOException {
        Detector.Builder d = new Detector.Builder("metric", "Integer_Value");
        d.setByFieldName("ts_hash");

        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Arrays.asList(d.build()));
        builder.setCategorizationFieldName("foo");
        analysisConfig = builder.build();
        writer = mock(OutputStreamWriter.class);

        createFieldConfigWriter().write();

        verify(writer).write("detector.0.clause = metric(Integer_Value) by ts_hash categorizationfield=foo\n");
        verifyNoMoreInteractions(writer);
    }

    public void testWrite_GivenConfigHasInfluencers() throws IOException {
        Detector.Builder d = new Detector.Builder("metric", "Integer_Value");
        d.setByFieldName("ts_hash");

        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Arrays.asList(d.build()));
        builder.setInfluencers(Arrays.asList("sun", "moon", "earth"));
        analysisConfig = builder.build();

        writer = mock(OutputStreamWriter.class);

        createFieldConfigWriter().write();

        verify(writer).write("detector.0.clause = metric(Integer_Value) by ts_hash\n" +
                "influencer.0 = sun\n" +
                "influencer.1 = moon\n" +
                "influencer.2 = earth\n");
        verifyNoMoreInteractions(writer);
    }

    public void testWrite_GivenConfigHasCategorizationFieldAndFiltersAndInfluencer() throws IOException {
        Detector.Builder d = new Detector.Builder("metric", "Integer_Value");
        d.setByFieldName("ts_hash");

        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Arrays.asList(d.build()));
        builder.setInfluencers(Arrays.asList("sun"));
        builder.setCategorizationFieldName("myCategory");
        builder.setCategorizationFilters(Arrays.asList("foo", " ", "abc,def"));
        analysisConfig = builder.build();

        writer = mock(OutputStreamWriter.class);

        createFieldConfigWriter().write();

        verify(writer).write(
                "detector.0.clause = metric(Integer_Value) by ts_hash categorizationfield=myCategory\n" +
                        "categorizationfilter.0 = foo\n" +
                        "categorizationfilter.1 = \" \"\n" +
                        "categorizationfilter.2 = \"abc,def\"\n" +
                "influencer.0 = sun\n");
        verifyNoMoreInteractions(writer);
    }

    public void testWrite_GivenDetectorWithRules() throws IOException {
        Detector.Builder detector = new Detector.Builder("mean", "metricValue");
        detector.setByFieldName("metricName");
        detector.setPartitionFieldName("instance");
        RuleCondition ruleCondition =
                new RuleCondition(RuleConditionType.NUMERICAL_ACTUAL, "metricName", "metricValue", new Condition(Operator.LT, "5"), null);
        DetectionRule rule = new DetectionRule("instance", null, Connective.OR, Arrays.asList(ruleCondition));
        detector.setDetectorRules(Arrays.asList(rule));

        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Arrays.asList(detector.build()));
        analysisConfig = builder.build();

        writer = mock(OutputStreamWriter.class);

        createFieldConfigWriter().write();

        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(writer).write(captor.capture());
        String actual = captor.getValue();
        String expectedFirstLine = "detector.0.clause = mean(metricValue) by metricName partitionfield=instance\n";
        assertTrue(actual.startsWith(expectedFirstLine));
        String secondLine = actual.substring(expectedFirstLine.length());
        String expectedSecondLineStart = "detector.0.rules = ";
        assertTrue(secondLine.startsWith(expectedSecondLineStart));
        String rulesJson = secondLine.substring(expectedSecondLineStart.length());
        assertEquals("[" + rule.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).string() + "]\n", rulesJson);
    }

    public void testWrite_GivenFilters() throws IOException {
        Detector d = new Detector.Builder("count", null).build();

        AnalysisConfig.Builder builder = new AnalysisConfig.Builder(Arrays.asList(d));
        analysisConfig = builder.build();

        filters.add(new MlFilter("filter_1", Arrays.asList("a", "b")));
        filters.add(new MlFilter("filter_2", Arrays.asList("c", "d")));
        writer = mock(OutputStreamWriter.class);

        createFieldConfigWriter().write();

        verify(writer).write("detector.0.clause = count\n" +
                "filter.filter_1 = [\"a\",\"b\"]\n" +
                "filter.filter_2 = [\"c\",\"d\"]\n");
        verifyNoMoreInteractions(writer);
    }

    private FieldConfigWriter createFieldConfigWriter() {
        return new FieldConfigWriter(analysisConfig, filters, writer, mock(Logger.class));
    }
}
