/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction.Response;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;

import java.util.ArrayList;
import java.util.List;

public class GetDataFrameAnalyticsStatsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<Response.Stats> analytics = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            Response.Stats stats = new Response.Stats(DataFrameAnalyticsConfigTests.randomValidId(),
                randomFrom(DataFrameAnalyticsState.values()), null, randomAlphaOfLength(20));
            analytics.add(stats);
        }
        return new Response(new QueryPage<>(analytics, analytics.size(), GetDataFrameAnalyticsAction.Response.RESULTS_FIELD));
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
