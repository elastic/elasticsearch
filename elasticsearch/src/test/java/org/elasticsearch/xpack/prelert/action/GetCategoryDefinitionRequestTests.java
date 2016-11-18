/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.job.results.PageParams;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

public class GetCategoryDefinitionRequestTests extends AbstractStreamableTestCase<GetCategoryDefinitionAction.Request> {

    @Override
    protected GetCategoryDefinitionAction.Request createTestInstance() {
        String jobId = randomAsciiOfLength(10);
        GetCategoryDefinitionAction.Request request = new GetCategoryDefinitionAction.Request(jobId);
        if (randomBoolean()) {
            request.setCategoryId(randomAsciiOfLength(10));
        } else {
            int from = randomInt(PageParams.MAX_FROM_SIZE_SUM);
            int maxSize = PageParams.MAX_FROM_SIZE_SUM - from;
            int size = randomInt(maxSize);
            request.setPageParams(new PageParams(from, size));
        }
        return request;
    }

    @Override
    protected GetCategoryDefinitionAction.Request createBlankInstance() {
        return new GetCategoryDefinitionAction.Request();
    }
}
