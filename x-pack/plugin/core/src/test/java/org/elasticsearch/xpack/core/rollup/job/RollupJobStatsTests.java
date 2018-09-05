/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.rollup.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.rollup.job.RollupJobStats;

public class RollupJobStatsTests extends AbstractSerializingTestCase<RollupJobStats> {

    @Override
    protected RollupJobStats createTestInstance() {
        return randomStats();
    }

    @Override
    protected Writeable.Reader<RollupJobStats> instanceReader() {
        return RollupJobStats::new;
    }

    @Override
    protected RollupJobStats doParseInstance(XContentParser parser) {
        return RollupJobStats.fromXContent(parser);
    }

    public static RollupJobStats randomStats() {
        return new RollupJobStats(randomNonNegativeLong(), randomNonNegativeLong(),
                randomNonNegativeLong(), randomNonNegativeLong());
    }
}

