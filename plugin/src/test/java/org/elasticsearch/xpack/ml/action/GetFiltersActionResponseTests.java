/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.ml.action.GetFiltersAction.Response;
import org.elasticsearch.xpack.ml.action.util.QueryPage;
import org.elasticsearch.xpack.ml.job.config.MlFilter;

import java.util.Collections;

public class GetFiltersActionResponseTests extends AbstractStreamableTestCase<GetFiltersAction.Response> {

    @Override
    protected Response createTestInstance() {
        final QueryPage<MlFilter> result;

        MlFilter doc = new MlFilter(
                randomAlphaOfLengthBetween(1, 20), Collections.singletonList(randomAlphaOfLengthBetween(1, 20)));
        result = new QueryPage<>(Collections.singletonList(doc), 1, MlFilter.RESULTS_FIELD);
        return new Response(result);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
