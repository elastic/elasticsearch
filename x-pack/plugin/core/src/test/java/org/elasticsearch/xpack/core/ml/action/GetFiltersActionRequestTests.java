/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.action.GetFiltersAction.Request;
import org.elasticsearch.xpack.core.ml.action.util.PageParams;

public class GetFiltersActionRequestTests extends AbstractStreamableTestCase<GetFiltersAction.Request> {


    @Override
    protected Request createTestInstance() {
        Request request = new Request();
        if (randomBoolean()) {
            request.setFilterId(randomAlphaOfLengthBetween(1, 20));
        } else {
            if (randomBoolean()) {
                int from = randomInt(10000);
                int size = randomInt(10000);
                request.setPageParams(new PageParams(from, size));
            }
        }
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

}
