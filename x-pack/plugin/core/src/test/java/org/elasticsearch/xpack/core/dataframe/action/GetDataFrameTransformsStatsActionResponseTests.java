/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.xpack.core.dataframe.action.GetDataFrameTransformsStatsAction.Response;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStats;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameTransformStateAndStatsTests;

import java.util.ArrayList;
import java.util.List;

public class GetDataFrameTransformsStatsActionResponseTests extends AbstractWireSerializingDataFrameTestCase<Response> {
    @Override
    protected Response createTestInstance() {
        List<DataFrameTransformStateAndStats> stats = new ArrayList<>();
        for (int i = 0; i < randomInt(10); ++i) {
            stats.add(DataFrameTransformStateAndStatsTests.randomDataFrameTransformStateAndStats());
        }

        return new Response(stats);
    }

    @Override
    protected Reader<Response> instanceReader() {
        return Response::new;
    }
}
