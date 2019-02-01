/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalysisConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DataFrameAnalysesUtilsTests extends ESTestCase {

    public void testReadAnalysis_GivenEmptyAnalysisList() {
        assertThat(DataFrameAnalysesUtils.readAnalyses(Collections.emptyList()).isEmpty(), is(true));
    }

    public void testReadAnalysis_GivenUnknownAnalysis() {
        String analysisJson = "{\"unknown_analysis\": {}}";
        DataFrameAnalysisConfig analysisConfig = createAnalysisConfig(analysisJson);

        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> DataFrameAnalysesUtils.readAnalyses(Collections.singletonList(analysisConfig)));

        assertThat(e.getMessage(), equalTo("Unknown analysis type [unknown_analysis]"));
    }

    public void testReadAnalysis_GivenAnalysisIsNotAnObject() {
        String analysisJson = "{\"outlier_detection\": 42}";
        DataFrameAnalysisConfig analysisConfig = createAnalysisConfig(analysisJson);

        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> DataFrameAnalysesUtils.readAnalyses(Collections.singletonList(analysisConfig)));

        assertThat(e.getMessage(), equalTo("[outlier_detection] expected to be a map but was of type [java.lang.Integer]"));
    }

    public void testReadAnalysis_GivenUnusedParameters() {
        String analysisJson = "{\"outlier_detection\": {\"number_neighbours\":42, \"foo\": 1}}";
        DataFrameAnalysisConfig analysisConfig = createAnalysisConfig(analysisJson);

        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> DataFrameAnalysesUtils.readAnalyses(Collections.singletonList(analysisConfig)));

        assertThat(e.getMessage(), equalTo("Data frame analysis [outlier_detection] does not support one or more provided " +
            "parameters [foo]"));
    }

    public void testReadAnalysis_GivenValidOutlierDetection() {
        String analysisJson = "{\"outlier_detection\": {\"number_neighbours\":42}}";
        DataFrameAnalysisConfig analysisConfig = createAnalysisConfig(analysisJson);

        List<DataFrameAnalysis> analyses = DataFrameAnalysesUtils.readAnalyses(Collections.singletonList(analysisConfig));

        assertThat(analyses.size(), equalTo(1));
        assertThat(analyses.get(0), is(instanceOf(OutlierDetection.class)));
        OutlierDetection outlierDetection = (OutlierDetection) analyses.get(0);
        assertThat(outlierDetection.getParams().size(), equalTo(1));
        assertThat(outlierDetection.getParams().get("number_neighbours"), equalTo(42));
    }

    private static DataFrameAnalysisConfig createAnalysisConfig(String json) {
        Map<String, Object> asMap = XContentHelper.convertToMap(new BytesArray(json), true, XContentType.JSON).v2();
        return new DataFrameAnalysisConfig(asMap);
    }
}
