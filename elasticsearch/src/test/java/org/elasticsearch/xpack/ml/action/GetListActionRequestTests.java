/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.action.GetListAction.Request;
import org.elasticsearch.xpack.ml.action.util.PageParams;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

public class GetListActionRequestTests extends AbstractStreamableTestCase<GetListAction.Request> {


    @Override
    protected Request createTestInstance() {
        Request request = new Request();
        if (randomBoolean()) {
            request.setListId(randomAsciiOfLengthBetween(1, 20));
        } else {
            if (randomBoolean()) {
                int from = randomInt(PageParams.MAX_FROM_SIZE_SUM);
                int maxSize = PageParams.MAX_FROM_SIZE_SUM - from;
                int size = randomInt(maxSize);
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
