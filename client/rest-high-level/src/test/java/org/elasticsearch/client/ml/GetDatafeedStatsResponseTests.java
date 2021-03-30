/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.datafeed.DatafeedStats;
import org.elasticsearch.client.ml.datafeed.DatafeedStatsTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public class GetDatafeedStatsResponseTests extends AbstractXContentTestCase<GetDatafeedStatsResponse> {

    @Override
    protected GetDatafeedStatsResponse createTestInstance() {

        int count = randomIntBetween(1, 5);
        List<DatafeedStats> results = new ArrayList<>(count);
        for(int i = 0; i < count; i++) {
            results.add(DatafeedStatsTests.createRandomInstance());
        }

        return new GetDatafeedStatsResponse(results, count);
    }

    @Override
    protected GetDatafeedStatsResponse doParseInstance(XContentParser parser) throws IOException {
        return GetDatafeedStatsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.isEmpty() == false;
    }
}
