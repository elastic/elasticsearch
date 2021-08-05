/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action.schema;

import org.elasticsearch.test.AbstractSchemaValidationTestCase;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction.Response;

import static org.elasticsearch.xpack.core.transform.action.GetTransformStatsActionResponseTests.randomTransformStatsResponse;

public class GetTransformStatsActionResponseTests extends AbstractSchemaValidationTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        return randomTransformStatsResponse();
    }

    @Override
    protected String getJsonSchemaFileName() {
        return "transform.get_transform_stats.schema.json";
    }
}
