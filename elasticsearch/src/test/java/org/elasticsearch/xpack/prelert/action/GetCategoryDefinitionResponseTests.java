/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.elasticsearch.xpack.prelert.job.results.CategoryDefinition;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

import java.util.Collections;

public class GetCategoryDefinitionResponseTests extends AbstractStreamableTestCase<GetCategoriesDefinitionAction.Response> {

    @Override
    protected GetCategoriesDefinitionAction.Response createTestInstance() {
        CategoryDefinition definition = new CategoryDefinition(randomAsciiOfLength(10));
        QueryPage<CategoryDefinition> queryPage =
                new QueryPage<>(Collections.singletonList(definition), 1L, CategoryDefinition.RESULTS_FIELD);
        return new GetCategoriesDefinitionAction.Response(queryPage);
    }

    @Override
    protected GetCategoriesDefinitionAction.Response createBlankInstance() {
        return new GetCategoriesDefinitionAction.Response();
    }
}
