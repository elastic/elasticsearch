/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.action.GetJobAction.Request;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

public class GetJobActionRequestTests extends AbstractStreamableTestCase<GetJobAction.Request> {

    @Override
    protected Request createTestInstance() {
        Request instance = new Request(randomAsciiOfLengthBetween(1, 20));
        instance.config(randomBoolean());
        instance.dataCounts(randomBoolean());
        instance.modelSizeStats(randomBoolean());
        return instance;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

}
