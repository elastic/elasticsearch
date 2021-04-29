/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.schema;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.test.AbstractSchemaValidationTestCase;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;

import java.util.Collections;

import static org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests.randomTransformConfig;

public class TransformConfigTests extends AbstractSchemaValidationTestCase<TransformConfig> {

    protected static Params TO_XCONTENT_PARAMS = new ToXContent.MapParams(
        Collections.singletonMap(TransformField.EXCLUDE_GENERATED, "true")
    );

    @Override
    protected TransformConfig createTestInstance() {
        return randomTransformConfig();
    }

    @Override
    protected String getJsonSchemaFileName() {
        return "transform_config.schema.json";
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return TO_XCONTENT_PARAMS;
    }
}
