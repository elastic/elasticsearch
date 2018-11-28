/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class DataFrameIndexerJobStatsTests extends AbstractSerializingTestCase<DataFrameIndexerJobStats>{
    @Override
    protected DataFrameIndexerJobStats createTestInstance() {
        return randomStats();
    }

    @Override
    protected Writeable.Reader<DataFrameIndexerJobStats> instanceReader() {
        return DataFrameIndexerJobStats::new;
    }

    @Override
    protected DataFrameIndexerJobStats doParseInstance(XContentParser parser) {
        return DataFrameIndexerJobStats.fromXContent(parser);
    }

    public static DataFrameIndexerJobStats randomStats() {
        return new DataFrameIndexerJobStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
                randomNonNegativeLong(), randomNonNegativeLong());
    }
}
