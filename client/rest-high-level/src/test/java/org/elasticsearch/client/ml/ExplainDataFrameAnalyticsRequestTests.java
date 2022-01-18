/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ExplainDataFrameAnalyticsRequestTests extends ESTestCase {

    public void testIdConstructor() {
        ExplainDataFrameAnalyticsRequest request = new ExplainDataFrameAnalyticsRequest("foo");
        assertThat(request.getId(), equalTo("foo"));
        assertThat(request.getConfig(), is(nullValue()));
    }

    public void testConfigConstructor() {
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfigTests.randomDataFrameAnalyticsConfig();

        ExplainDataFrameAnalyticsRequest request = new ExplainDataFrameAnalyticsRequest(config);
        assertThat(request.getId(), is(nullValue()));
        assertThat(request.getConfig(), equalTo(config));
    }
}
