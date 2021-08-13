/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class DocsStatsTests extends ESTestCase {

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
}
