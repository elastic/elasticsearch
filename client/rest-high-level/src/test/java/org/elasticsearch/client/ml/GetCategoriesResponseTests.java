/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.results.CategoryDefinition;
import org.elasticsearch.client.ml.job.results.CategoryDefinitionTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GetCategoriesResponseTests extends AbstractXContentTestCase<GetCategoriesResponse> {

    @Override
    protected GetCategoriesResponse createTestInstance() {
        String jobId = randomAlphaOfLength(20);
        int listSize = randomInt(10);
        List<CategoryDefinition> categories = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            CategoryDefinition category = CategoryDefinitionTests.createTestInstance(jobId);
            categories.add(category);
        }
        return new GetCategoriesResponse(categories, listSize);
    }

    @Override
    protected GetCategoriesResponse doParseInstance(XContentParser parser) throws IOException {
        return GetCategoriesResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
