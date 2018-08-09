/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class IndexerStatsTests extends AbstractSerializingTestCase<IndexerStats> {

    @Override
    protected IndexerStats createTestInstance() {
        return randomStats();
    }

    @Override
    protected Writeable.Reader<IndexerStats> instanceReader() {
        return IndexerStats::new;
    }

    @Override
    protected IndexerStats doParseInstance(XContentParser parser) {
        return IndexerStats.fromXContent(parser);
    }

    public static IndexerStats randomStats() {
        return new IndexerStats(randomNonNegativeLong(), randomNonNegativeLong(),
                randomNonNegativeLong(), randomNonNegativeLong());
    }
}

