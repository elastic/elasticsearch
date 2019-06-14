/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerTransformStatsTests;
import org.elasticsearch.xpack.core.indexing.IndexerState;

import java.util.HashMap;
import java.util.Map;

public class DataFrameFeatureSetUsageTests extends AbstractWireSerializingTestCase<DataFrameFeatureSetUsage> {

    @Override
    protected DataFrameFeatureSetUsage createTestInstance() {
        Map<String, Long> transformCountByState = new HashMap<>();

        if (randomBoolean()) {
            transformCountByState.put(randomFrom(IndexerState.values()).toString(), randomLong());
        }

        return new DataFrameFeatureSetUsage(randomBoolean(), randomBoolean(), transformCountByState,
                DataFrameIndexerTransformStatsTests.randomStats());
    }

    @Override
    protected Reader<DataFrameFeatureSetUsage> instanceReader() {
        return DataFrameFeatureSetUsage::new;
    }

}
