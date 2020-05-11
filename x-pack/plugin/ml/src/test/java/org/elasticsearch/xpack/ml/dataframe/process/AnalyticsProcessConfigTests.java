/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Classification;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.DataFrameAnalysis;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.OutlierDetection;
import org.elasticsearch.xpack.core.ml.dataframe.analyses.Regression;
import org.elasticsearch.xpack.ml.extractor.DocValueField;
import org.elasticsearch.xpack.ml.extractor.ExtractedFields;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;

public class AnalyticsProcessConfigTests extends ESTestCase {

    private String jobId;
    private long rows;
    private int cols;
    private ByteSizeValue memoryLimit;
    private int threads;
    private String resultsField;
    private Set<String> categoricalFields;

    @Before
    public void setUpConfigParams() {
        jobId = randomAlphaOfLength(10);
        rows = randomNonNegativeLong();
        cols = randomIntBetween(1, 42000);
        memoryLimit = new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES);
        threads = randomIntBetween(1, 8);
        resultsField = randomAlphaOfLength(10);

        int categoricalFieldsSize = randomIntBetween(0, 5);
        categoricalFields = new HashSet<>();
        for (int i = 0; i < categoricalFieldsSize; i++) {
            categoricalFields.add(randomAlphaOfLength(10));
        }
    }

    @SuppressWarnings("unchecked")
    public void testToXContent_GivenOutlierDetection() throws IOException {
        ExtractedFields extractedFields = new ExtractedFields(Arrays.asList(
            new DocValueField("field_1", Collections.singleton("double")),
            new DocValueField("field_2", Collections.singleton("float"))), Collections.emptyMap());
        DataFrameAnalysis analysis = new OutlierDetection.Builder().build();

        AnalyticsProcessConfig processConfig = createProcessConfig(analysis, extractedFields);
        Map<String, Object> asMap = toMap(processConfig);

        assertRandomizedFields(asMap);

        assertThat(asMap, hasKey("analysis"));
        Map<String, Object> analysisAsMap = (Map<String, Object>) asMap.get("analysis");
        assertThat(analysisAsMap, hasEntry("name", "outlier_detection"));
        assertThat(analysisAsMap, hasKey("parameters"));
    }

    @SuppressWarnings("unchecked")
    public void testToXContent_GivenRegression() throws IOException {
        ExtractedFields extractedFields = new ExtractedFields(Arrays.asList(
            new DocValueField("field_1", Collections.singleton("double")),
            new DocValueField("field_2", Collections.singleton("float")),
            new DocValueField("test_dep_var", Collections.singleton("keyword"))), Collections.emptyMap());
        DataFrameAnalysis analysis = new Regression("test_dep_var");

        AnalyticsProcessConfig processConfig = createProcessConfig(analysis, extractedFields);
        Map<String, Object> asMap = toMap(processConfig);

        assertRandomizedFields(asMap);

        assertThat(asMap, hasKey("analysis"));
        Map<String, Object> analysisAsMap = (Map<String, Object>) asMap.get("analysis");
        assertThat(analysisAsMap, hasEntry("name", "regression"));
        assertThat(analysisAsMap, hasKey("parameters"));
        Map<String, Object> paramsAsMap = (Map<String, Object>) analysisAsMap.get("parameters");
        assertThat(paramsAsMap, hasEntry("dependent_variable", "test_dep_var"));
    }

    @SuppressWarnings("unchecked")
    public void testToXContent_GivenClassificationAndDepVarIsKeyword() throws IOException {
        ExtractedFields extractedFields = new ExtractedFields(Arrays.asList(
            new DocValueField("field_1", Collections.singleton("double")),
            new DocValueField("field_2", Collections.singleton("float")),
            new DocValueField("test_dep_var", Collections.singleton("keyword"))), Collections.singletonMap("test_dep_var", 5L));
        DataFrameAnalysis analysis = new Classification("test_dep_var");

        AnalyticsProcessConfig processConfig = createProcessConfig(analysis, extractedFields);
        Map<String, Object> asMap = toMap(processConfig);

        assertRandomizedFields(asMap);

        assertThat(asMap, hasKey("analysis"));
        Map<String, Object> analysisAsMap = (Map<String, Object>) asMap.get("analysis");
        assertThat(analysisAsMap, hasEntry("name", "classification"));
        assertThat(analysisAsMap, hasKey("parameters"));
        Map<String, Object> paramsAsMap = (Map<String, Object>) analysisAsMap.get("parameters");
        assertThat(paramsAsMap, hasEntry("dependent_variable", "test_dep_var"));
        assertThat(paramsAsMap, hasEntry("prediction_field_type", "string"));
        assertThat(paramsAsMap, hasEntry("num_classes", 5));
    }

    @SuppressWarnings("unchecked")
    public void testToXContent_GivenClassificationAndDepVarIsInteger() throws IOException {
        ExtractedFields extractedFields = new ExtractedFields(Arrays.asList(
            new DocValueField("field_1", Collections.singleton("double")),
            new DocValueField("field_2", Collections.singleton("float")),
            new DocValueField("test_dep_var", Collections.singleton("integer"))), Collections.singletonMap("test_dep_var", 8L));
        DataFrameAnalysis analysis = new Classification("test_dep_var");

        AnalyticsProcessConfig processConfig = createProcessConfig(analysis, extractedFields);
        Map<String, Object> asMap = toMap(processConfig);

        assertRandomizedFields(asMap);

        assertThat(asMap, hasKey("analysis"));
        Map<String, Object> analysisAsMap = (Map<String, Object>) asMap.get("analysis");
        assertThat(analysisAsMap, hasEntry("name", "classification"));
        assertThat(analysisAsMap, hasKey("parameters"));
        Map<String, Object> paramsAsMap = (Map<String, Object>) analysisAsMap.get("parameters");
        assertThat(paramsAsMap, hasEntry("dependent_variable", "test_dep_var"));
        assertThat(paramsAsMap, hasEntry("prediction_field_type", "int"));
        assertThat(paramsAsMap, hasEntry("num_classes", 8));
    }

    private AnalyticsProcessConfig createProcessConfig(DataFrameAnalysis analysis, ExtractedFields extractedFields) {
        return new AnalyticsProcessConfig(jobId, rows, cols, memoryLimit, threads, resultsField, categoricalFields, analysis,
            extractedFields);
    }

    private static Map<String, Object> toMap(AnalyticsProcessConfig config) throws IOException {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            config.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, Strings.toString(builder), false);
        }
    }

    @SuppressWarnings("unchecked")
    private void assertRandomizedFields(Map<String, Object> configAsMap) {
        assertThat(configAsMap, hasEntry("job_id", jobId));
        assertThat(configAsMap, hasEntry("rows", rows));
        assertThat(configAsMap, hasEntry("cols", cols));
        assertThat(configAsMap, hasEntry("memory_limit", memoryLimit.getBytes()));
        assertThat(configAsMap, hasEntry("threads", threads));
        assertThat(configAsMap, hasEntry("results_field", resultsField));
        assertThat(configAsMap, hasKey("categorical_fields"));
        assertThat((List<String>) configAsMap.get("categorical_fields"), containsInAnyOrder(categoricalFields.toArray()));
    }
}
