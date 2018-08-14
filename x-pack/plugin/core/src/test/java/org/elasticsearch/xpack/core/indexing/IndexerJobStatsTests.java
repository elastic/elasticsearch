/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexing;

import com.carrotsearch.randomizedtesting.WriterOutputStream;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;

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
        return new IndexerJobStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    public void testDeprecation() throws IOException {
        IndexerJobStats stats = randomStats();

        assertTrue(toJson(stats, true).indexOf(IndexerJobStats.ROLLUP_BWC_NUM_OUTPUT_DOCUMENTS) > 0);
        assertFalse(toJson(stats, false).indexOf(IndexerJobStats.ROLLUP_BWC_NUM_OUTPUT_DOCUMENTS) > 0);
    }

    private String toJson(IndexerJobStats stats, boolean bwc) throws IOException {
        final StringWriter writer = new StringWriter();
        try (XContentBuilder builder = XContentFactory.jsonBuilder(new WriterOutputStream(writer))) {
            ToXContent.Params params = new ToXContent.MapParams(
                    Collections.singletonMap(IndexerJobStats.ROLLUP_BWC_XCONTENT_PARAM, String.valueOf(bwc)));
            stats.toXContent(builder, params);
        }
        return writer.toString();
    }
}
