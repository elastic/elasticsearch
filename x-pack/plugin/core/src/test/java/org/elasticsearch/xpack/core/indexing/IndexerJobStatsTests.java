/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexing;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

public class IndexerJobStatsTests extends AbstractSerializingTestCase<IndexerJobStats> {

    @Override
    protected IndexerJobStats createTestInstance() {
        return randomStats();
    }

    @Override
    protected Writeable.Reader<IndexerJobStats> instanceReader() {
        return IndexerJobStats::new;
    }

    @Override
    protected IndexerJobStats doParseInstance(XContentParser parser) {
        return IndexerJobStats.fromXContent(parser);
    }

    public static IndexerJobStats randomStats() {
        return new IndexerJobStats(randomNonNegativeLong(), randomNonNegativeLong(),
                randomNonNegativeLong(), randomNonNegativeLong());
    }
}

