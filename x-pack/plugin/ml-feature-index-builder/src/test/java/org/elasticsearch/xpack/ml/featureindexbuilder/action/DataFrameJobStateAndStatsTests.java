/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.featureindexbuilder.action;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.AbstractSerializingFeatureIndexBuilderTestCase;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.DataFrameIndexerJobStatsTests;
import org.elasticsearch.xpack.ml.featureindexbuilder.job.FeatureIndexBuilderJobStateTests;

import java.io.IOException;

public class DataFrameJobStateAndStatsTests
        extends AbstractSerializingFeatureIndexBuilderTestCase<DataFrameJobStateAndStats> {

    @Override
    protected DataFrameJobStateAndStats doParseInstance(XContentParser parser) throws IOException {
        return DataFrameJobStateAndStats.PARSER.apply(parser, null);
    }

    @Override
    protected DataFrameJobStateAndStats createTestInstance() {
        return new DataFrameJobStateAndStats(randomAlphaOfLengthBetween(1,10),
                FeatureIndexBuilderJobStateTests.randomFeatureIndexBuilderJobState(),
                DataFrameIndexerJobStatsTests.randomStats());
    }

    @Override
    protected Reader<DataFrameJobStateAndStats> instanceReader() {
        return DataFrameJobStateAndStats::new;
    }

}
