/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.EvaluateDataFrameAction.Response;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.EvaluationMetricResult;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.xpack.core.ml.dataframe.evaluation.regression.MeanSquaredError;

import java.util.List;

public class EvaluateDataFrameActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new MlEvaluationNamedXContentProvider().getNamedWriteables());
    }

    @Override
    protected Response createTestInstance() {
        String evaluationName = randomAlphaOfLength(10);
        List<EvaluationMetricResult> metrics = List.of(new MeanSquaredError.Result(1.0));
        return new Response(evaluationName, metrics);
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
