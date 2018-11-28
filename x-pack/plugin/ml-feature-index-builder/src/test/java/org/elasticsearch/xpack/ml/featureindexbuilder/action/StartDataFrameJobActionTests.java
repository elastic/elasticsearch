package org.elasticsearch.xpack.ml.featureindexbuilder.action;
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.rollup.action.StartRollupJobAction.Request;

public class StartDataFrameJobActionTests extends AbstractStreamableTestCase<Request> {
    @Override
    protected Request createTestInstance() {
        return new Request(randomAlphaOfLengthBetween(1, 20));
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }
}
