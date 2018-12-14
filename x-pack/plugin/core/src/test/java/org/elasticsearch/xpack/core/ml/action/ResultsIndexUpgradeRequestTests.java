/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;


public class ResultsIndexUpgradeRequestTests extends AbstractWireSerializingTestCase<ResultsIndexUpgradeAction.Request> {

    @Override
    protected ResultsIndexUpgradeAction.Request createTestInstance() {
        ResultsIndexUpgradeAction.Request request = new ResultsIndexUpgradeAction.Request();
        if (randomBoolean()) {
            request.setReindexBatchSize(randomIntBetween(1, 10_000));
        }
        return request;
    }

    @Override
    protected Writeable.Reader<ResultsIndexUpgradeAction.Request> instanceReader() {
        return ResultsIndexUpgradeAction.Request::new;
    }

}
