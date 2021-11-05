/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction.Response;
import org.elasticsearch.xpack.core.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.xpack.core.ml.dataframe.explain.FieldSelectionTests;
import org.elasticsearch.xpack.core.ml.dataframe.explain.MemoryEstimation;
import org.elasticsearch.xpack.core.ml.dataframe.explain.MemoryEstimationTests;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class ExplainDataFrameAnalyticsActionResponseTests extends AbstractSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        int fieldSelectionCount = randomIntBetween(1, 5);
        List<FieldSelection> fieldSelection = new ArrayList<>(fieldSelectionCount);
        IntStream.of(fieldSelectionCount).forEach(i -> fieldSelection.add(FieldSelectionTests.createRandom()));
        MemoryEstimation memoryEstimation = MemoryEstimationTests.createRandom();

        return new Response(fieldSelection, memoryEstimation);
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response doParseInstance(XContentParser parser) {
        return Response.PARSER.apply(parser, null);
    }
}
