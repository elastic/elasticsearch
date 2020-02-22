/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetDataFrameAnalyticsStatsAction.Response;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfigTests;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.utils.PhaseProgress;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class GetDataFrameAnalyticsStatsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    public static Response randomResponse(int listSize) {
        List<Response.Stats> analytics = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            String failureReason = randomBoolean() ? null : randomAlphaOfLength(10);
            int progressSize = randomIntBetween(2, 5);
            List<PhaseProgress> progress = new ArrayList<>(progressSize);
            IntStream.of(progressSize).forEach(progressIndex -> progress.add(
                new PhaseProgress(randomAlphaOfLength(10), randomIntBetween(0, 100))));
            Response.Stats stats = new Response.Stats(DataFrameAnalyticsConfigTests.randomValidId(),
                randomFrom(DataFrameAnalyticsState.values()), failureReason, progress, null, randomAlphaOfLength(20));
            analytics.add(stats);
        }
        return new Response(new QueryPage<>(analytics, analytics.size(), GetDataFrameAnalyticsAction.Response.RESULTS_FIELD));
    }

    @Override
    protected Response createTestInstance() {
        return randomResponse(randomInt(10));
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
