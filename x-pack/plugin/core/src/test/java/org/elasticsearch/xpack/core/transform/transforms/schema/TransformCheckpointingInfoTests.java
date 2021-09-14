/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.schema;

import org.elasticsearch.test.AbstractSchemaValidationTestCase;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;

import static org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfoTests.randomTransformCheckpointingInfo;

public class TransformCheckpointingInfoTests extends AbstractSchemaValidationTestCase<TransformCheckpointingInfo> {

    @Override
    protected TransformCheckpointingInfo createTestInstance() {
        return randomTransformCheckpointingInfo();
    }

    @Override
    protected String getJsonSchemaFileName() {
        return "transform_checkpointing_info.schema.json";
    }

}
