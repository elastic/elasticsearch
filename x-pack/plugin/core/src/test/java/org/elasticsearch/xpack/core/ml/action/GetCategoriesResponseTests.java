/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;

import java.util.Collections;

public class GetCategoriesResponseTests extends AbstractStreamableTestCase<GetCategoriesAction.Response> {

    @Override
    protected GetCategoriesAction.Response createTestInstance() {
        CategoryDefinition definition = new CategoryDefinition(randomAlphaOfLength(10));
        QueryPage<CategoryDefinition> queryPage =
                new QueryPage<>(Collections.singletonList(definition), 1L, CategoryDefinition.RESULTS_FIELD);
        return new GetCategoriesAction.Response(queryPage);
    }

    @Override
    protected GetCategoriesAction.Response createBlankInstance() {
        return new GetCategoriesAction.Response();
    }
}
