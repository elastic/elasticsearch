/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class ShardFieldStatsTests extends AbstractXContentTestCase<ShardFieldStats> {

    @Override
    protected ShardFieldStats createTestInstance() {
        return new ShardFieldStats(
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    @Override
    protected ShardFieldStats doParseInstance(XContentParser parser) throws IOException {
        return ShardFieldStats.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testParseWithoutPointsInMemoryBytes() throws IOException {
        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(XContentParserConfiguration.EMPTY, """
            {
              "shard_field_stats": {
                "num_segments": 2,
                "total_fields": 5,
                "field_usages": 10,
                "postings_in_memory_bytes": 100,
                "live_docs_bytes": 200
              }
            }
            """)) {
            ShardFieldStats stats = ShardFieldStats.PARSER.parse(parser, null);
            assertThat(stats.numSegments(), equalTo(2));
            assertThat(stats.totalFields(), equalTo(5));
            assertThat(stats.fieldUsages(), equalTo(10L));
            assertThat(stats.postingsInMemoryBytes(), equalTo(100L));
            assertThat(stats.liveDocsBytes(), equalTo(200L));
            assertThat(stats.pointsInMemoryBytes(), equalTo(0L));
        }
    }
}
