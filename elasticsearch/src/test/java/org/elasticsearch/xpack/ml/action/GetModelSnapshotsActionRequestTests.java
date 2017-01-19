/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.GetModelSnapshotsAction.Request;
import org.elasticsearch.xpack.ml.job.results.PageParams;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;

public class GetModelSnapshotsActionRequestTests extends AbstractStreamableXContentTestCase<GetModelSnapshotsAction.Request> {

    @Override
    protected Request parseInstance(XContentParser parser) {
        return GetModelSnapshotsAction.Request.parseRequest(null, null, parser);
    }

    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomAsciiOfLengthBetween(1, 20), randomBoolean() ? null : randomAsciiOfLengthBetween(1, 20));
        if (randomBoolean()) {
            request.setDescriptionString(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setStart(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setEnd(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setSort(randomAsciiOfLengthBetween(1, 20));
        }
        if (randomBoolean()) {
            request.setDescOrder(randomBoolean());
        }
        if (randomBoolean()) {
            int from = randomInt(PageParams.MAX_FROM_SIZE_SUM);
            int maxSize = PageParams.MAX_FROM_SIZE_SUM - from;
            int size = randomInt(maxSize);
            request.setPageParams(new PageParams(from, size));
        }
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

}
