/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction.Response;
import org.elasticsearch.xpack.core.ml.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;
import org.elasticsearch.xpack.core.ml.job.config.MlFilterTests;

import java.util.Collections;

public class GetFiltersActionResponseTests extends AbstractStreamableTestCase<GetFiltersAction.Response> {

    @Override
    protected Response createTestInstance() {
        final QueryPage<MlFilter> result;
        MlFilter doc = MlFilterTests.createRandom();
        result = new QueryPage<>(Collections.singletonList(doc), 1, MlFilter.RESULTS_FIELD);
        return new Response(result);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
