/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;

public class ResultsIndexUpgradeRequestTests extends AbstractStreamableTestCase<ResultsIndexUpgradeAction.Request> {

    @Override
    protected ResultsIndexUpgradeAction.Request createTestInstance() {
        ResultsIndexUpgradeAction.Request request = new ResultsIndexUpgradeAction.Request();
        request.setReindexBatchSize(randomIntBetween(1, 10_000));
        request.setShouldStoreResult(randomBoolean());
        return request;
    }

    @Override
    protected ResultsIndexUpgradeAction.Request createBlankInstance() {
        return new ResultsIndexUpgradeAction.Request();
    }
}
