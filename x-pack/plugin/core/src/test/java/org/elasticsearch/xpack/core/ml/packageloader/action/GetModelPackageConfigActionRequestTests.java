/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.packageloader.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class GetModelPackageConfigActionRequestTests extends AbstractWireSerializingTestCase<GetTrainedModelPackageConfigAction.Request> {

    public static GetTrainedModelPackageConfigAction.Request randomGetModelPackageConfigRequest() {
        return new GetTrainedModelPackageConfigAction.Request(randomAlphaOfLength(10));
    }

    @Override
    protected Writeable.Reader<GetTrainedModelPackageConfigAction.Request> instanceReader() {
        return GetTrainedModelPackageConfigAction.Request::new;
    }

    @Override
    protected GetTrainedModelPackageConfigAction.Request createTestInstance() {
        return randomGetModelPackageConfigRequest();
    }

    @Override
    protected GetTrainedModelPackageConfigAction.Request mutateInstance(GetTrainedModelPackageConfigAction.Request instance) {
        return new GetTrainedModelPackageConfigAction.Request(instance.getPackagedModelId() + randomAlphaOfLengthBetween(1, 5));
    }

}
