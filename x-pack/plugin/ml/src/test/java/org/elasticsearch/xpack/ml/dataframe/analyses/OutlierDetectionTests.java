/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.dataframe.analyses;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class OutlierDetectionTests extends ESTestCase {

    public void testCreate_GivenNumberNeighboursNotInt() {
        Map<String, Object> config = new HashMap<>();
        config.put(OutlierDetection.NUMBER_NEIGHBOURS, "42");

        DataFrameAnalysis.Factory factory = new OutlierDetection.Factory();

        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> factory.create(config));
        assertThat(e.getMessage(), equalTo("Property [number_neighbours] of analysis [outlier_detection] should be of " +
            "type [Integer] but was [String]"));
    }

    public void testCreate_GivenMethodNotString() {
        Map<String, Object> config = new HashMap<>();
        config.put(OutlierDetection.METHOD, 42);

        DataFrameAnalysis.Factory factory = new OutlierDetection.Factory();

        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> factory.create(config));
        assertThat(e.getMessage(), equalTo("Property [method] of analysis [outlier_detection] should be of " +
            "type [String] but was [Integer]"));
    }

    public void testCreate_GivenEmptyParams() {
        DataFrameAnalysis.Factory factory = new OutlierDetection.Factory();
        OutlierDetection outlierDetection = (OutlierDetection) factory.create(Collections.emptyMap());
        assertThat(outlierDetection.getParams().isEmpty(), is(true));
    }

    public void testCreate_GivenFullParams() {
        Map<String, Object> config = new HashMap<>();
        config.put(OutlierDetection.NUMBER_NEIGHBOURS, 42);
        config.put(OutlierDetection.METHOD, "ldof");

        DataFrameAnalysis.Factory factory = new OutlierDetection.Factory();
        OutlierDetection outlierDetection = (OutlierDetection) factory.create(config);

        assertThat(outlierDetection.getParams().size(), equalTo(2));
        assertThat(outlierDetection.getParams().get(OutlierDetection.NUMBER_NEIGHBOURS), equalTo(42));
        assertThat(outlierDetection.getParams().get(OutlierDetection.METHOD), equalTo(OutlierDetection.Method.LDOF));
    }
}
