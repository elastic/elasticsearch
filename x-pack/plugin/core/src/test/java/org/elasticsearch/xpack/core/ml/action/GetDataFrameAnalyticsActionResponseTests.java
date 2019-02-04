/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsAction.Response;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;

import java.util.ArrayList;
import java.util.List;

public class GetDataFrameAnalyticsActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<DataFrameAnalyticsConfig> analytics = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            analytics.add(DataFrameAnalyticsConfigTests.createRandom(DataFrameAnalyticsConfigTests.randomValidId()));
        }
        return new Response(new QueryPage<>(analytics, analytics.size(), Response.RESULTS_FIELD));
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }
}
