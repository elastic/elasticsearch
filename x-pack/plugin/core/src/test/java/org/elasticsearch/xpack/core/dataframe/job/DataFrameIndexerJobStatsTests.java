/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.dataframe.job;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class DataFrameIndexerJobStatsTests extends AbstractSerializingTestCase<DataFrameIndexerJobStats> {
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
        return new DataFrameIndexerJobStats(randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
                randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L),
                randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L), randomLongBetween(0L, 10000L));
    }

    public void testMerge() throws IOException {
        DataFrameIndexerJobStats emptyStats = new DataFrameIndexerJobStats();
        DataFrameIndexerJobStats randomStats = randomStats();

        assertEquals(randomStats, emptyStats.merge(randomStats));
        assertEquals(randomStats, randomStats.merge(emptyStats));

        DataFrameIndexerJobStats randomStatsClone = copyInstance(randomStats);

        DataFrameIndexerJobStats trippleRandomStats = new DataFrameIndexerJobStats(3 * randomStats.getNumPages(),
                3 * randomStats.getNumDocuments(), 3 * randomStats.getOutputDocuments(), 3 * randomStats.getNumInvocations(),
                3 * randomStats.getIndexTime(), 3 * randomStats.getSearchTime(), 3 * randomStats.getIndexTotal(),
                3 * randomStats.getSearchTotal(), 3 * randomStats.getIndexFailures(), 3 * randomStats.getSearchFailures());

        assertEquals(trippleRandomStats, randomStats.merge(randomStatsClone).merge(randomStatsClone));
    }
}
