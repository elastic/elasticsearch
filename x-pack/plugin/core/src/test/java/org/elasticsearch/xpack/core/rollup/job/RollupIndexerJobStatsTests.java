/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class RollupIndexerJobStatsTests extends AbstractSerializingTestCase<RollupIndexerJobStats> {

    @Override
    protected RollupIndexerJobStats createTestInstance() {
        return randomStats();
    }

    @Override
    protected Writeable.Reader<RollupIndexerJobStats> instanceReader() {
        return RollupIndexerJobStats::new;
    }

    @Override
    protected RollupIndexerJobStats doParseInstance(XContentParser parser) {
        return RollupIndexerJobStats.fromXContent(parser);
    }

    public static RollupIndexerJobStats randomStats() {
        return new RollupIndexerJobStats(randomNonNegativeLong(), randomNonNegativeLong(),
            randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
            randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(),
            randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
