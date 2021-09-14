/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.client.ml.dataframe.explain.FieldSelectionTests;
import org.elasticsearch.client.ml.dataframe.explain.MemoryEstimation;
import org.elasticsearch.client.ml.dataframe.explain.MemoryEstimationTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class ExplainDataFrameAnalyticsResponseTests extends AbstractXContentTestCase<ExplainDataFrameAnalyticsResponse> {

    @Override
    protected ExplainDataFrameAnalyticsResponse createTestInstance() {
        int fieldSelectionCount = randomIntBetween(1, 5);
        List<FieldSelection> fieldSelection = new ArrayList<>(fieldSelectionCount);
        IntStream.of(fieldSelectionCount).forEach(i -> fieldSelection.add(FieldSelectionTests.createRandom()));
        MemoryEstimation memoryEstimation = MemoryEstimationTests.createRandom();

        return new ExplainDataFrameAnalyticsResponse(fieldSelection, memoryEstimation);
    }

    @Override
    protected ExplainDataFrameAnalyticsResponse doParseInstance(XContentParser parser) throws IOException {
        return ExplainDataFrameAnalyticsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
