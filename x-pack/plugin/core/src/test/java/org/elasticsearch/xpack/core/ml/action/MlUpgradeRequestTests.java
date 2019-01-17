/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;


public class MlUpgradeRequestTests extends AbstractWireSerializingTestCase<MlUpgradeAction.Request> {

    @Override
    protected MlUpgradeAction.Request createTestInstance() {
        MlUpgradeAction.Request request = new MlUpgradeAction.Request();
        if (randomBoolean()) {
            request.setReindexBatchSize(randomIntBetween(1, 10_000));
        }
        return request;
    }

    @Override
    protected Writeable.Reader<MlUpgradeAction.Request> instanceReader() {
        return MlUpgradeAction.Request::new;
    }

}
