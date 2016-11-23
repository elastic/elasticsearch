/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.action.GetJobAction.Request;
import org.elasticsearch.xpack.prelert.job.results.PageParams;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

public class GetJobActionRequestTests extends AbstractStreamableTestCase<GetJobAction.Request> {

    @Override
    protected Request createTestInstance() {
        Request instance = new Request();
        instance.config(randomBoolean());
        instance.dataCounts(randomBoolean());
        instance.modelSizeStats(randomBoolean());
        instance.schedulerStatus(randomBoolean());
        instance.status(randomBoolean());
        if (randomBoolean()) {
            int from = randomInt(PageParams.MAX_FROM_SIZE_SUM);
            int maxSize = PageParams.MAX_FROM_SIZE_SUM - from;
            int size = randomInt(maxSize);
            instance.setPageParams(new PageParams(from, size));
        }
        if (randomBoolean()) {
            instance.setJobId(randomAsciiOfLengthBetween(1, 20));
        }
        return instance;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

}
