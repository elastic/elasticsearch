/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.schema;

import org.elasticsearch.test.xcontent.AbstractSchemaValidationTestCase;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats;

import static org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStatsTests.randomStats;

public class TransformIndexerStatsTests extends AbstractSchemaValidationTestCase<TransformIndexerStats> {

    @Override
    protected TransformIndexerStats createTestInstance() {
        return randomStats();
    }

    @Override
    protected String getJsonSchemaFileName() {
        return "transform_indexer_stats.schema.json";
    }

}
