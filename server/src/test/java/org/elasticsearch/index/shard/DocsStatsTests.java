/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class DocsStatsTests extends AbstractXContentTestCase<DocsStats> {

    public void testUninitialisedShards() {
        DocsStats stats = new DocsStats(0, 0, -1);
        assertThat(stats.getTotalSizeInBytes(), equalTo(-1L));
        stats.add(new DocsStats(0, 0, -1));
        assertThat(stats.getTotalSizeInBytes(), equalTo(-1L));
        stats.add(new DocsStats(1, 0, 10));
        assertThat(stats.getTotalSizeInBytes(), equalTo(10L));
        stats.add(new DocsStats(0, 0, -1));
        assertThat(stats.getTotalSizeInBytes(), equalTo(10L));
        stats.add(new DocsStats(1, 0, 20));
        assertThat(stats.getTotalSizeInBytes(), equalTo(30L));
    }

    public void testSerialize() throws Exception {
        DocsStats originalStats = new DocsStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalStats.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = bytes.streamInput()) {
                DocsStats cloneStats = new DocsStats(in);
                assertThat(cloneStats.getCount(), equalTo(originalStats.getCount()));
                assertThat(cloneStats.getDeleted(), equalTo(originalStats.getDeleted()));
                assertThat(cloneStats.getTotalSizeInBytes(), equalTo(originalStats.getTotalSizeInBytes()));
            }
        }
    }

    @Override
    protected DocsStats createTestInstance() {
        return new DocsStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected DocsStats doParseInstance(XContentParser parser) throws IOException {
        return DocsStats.PARSER.parse(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
