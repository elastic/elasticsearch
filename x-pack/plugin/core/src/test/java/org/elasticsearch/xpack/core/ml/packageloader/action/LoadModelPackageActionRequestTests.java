/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.packageloader.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.ModelPackageConfigTests;

public class LoadModelPackageActionRequestTests extends AbstractWireSerializingTestCase<LoadTrainedModelPackageAction.Request> {

    public static LoadTrainedModelPackageAction.Request randomLoadModelPackageRequest() {
        return new LoadTrainedModelPackageAction.Request(
            randomAlphaOfLength(10),
            ModelPackageConfigTests.randomModulePackageConfig(),
            randomBoolean()
        );
    }

    @Override
    protected Writeable.Reader<LoadTrainedModelPackageAction.Request> instanceReader() {
        return LoadTrainedModelPackageAction.Request::new;
    }

    @Override
    protected LoadTrainedModelPackageAction.Request createTestInstance() {
        return randomLoadModelPackageRequest();
    }

    @Override
    protected LoadTrainedModelPackageAction.Request mutateInstance(LoadTrainedModelPackageAction.Request instance) {
        return switch (between(0, 2)) {
            case 0 -> new LoadTrainedModelPackageAction.Request(
                instance.getModelId(),
                ModelPackageConfigTests.mutateModelPackageConfig(instance.getModelPackageConfig()),
                instance.isWaitForCompletion()
            );
            case 1 -> new LoadTrainedModelPackageAction.Request(
                randomAlphaOfLength(15),
                instance.getModelPackageConfig(),
                instance.isWaitForCompletion()
            );
            case 2 -> new LoadTrainedModelPackageAction.Request(
                instance.getModelId(),
                instance.getModelPackageConfig(),
                instance.isWaitForCompletion() == false
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        };
    }
}
