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

public class IgnoredFieldStatsTests extends ESTestCase {

    public void testUninitialized() {

        final IgnoredFieldStats ignoredFieldStats = new IgnoredFieldStats();
        assertThat(ignoredFieldStats.getTotalNumberOfDocuments(), equalTo(0L));
        assertThat(ignoredFieldStats.getDocsWithIgnoredFields(), equalTo(0L));
        assertThat(ignoredFieldStats.getIgnoredFieldTermsSumDocFreq(), equalTo(0L));
    }

    public void testSerialize() throws Exception {
        IgnoredFieldStats original = new IgnoredFieldStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            BytesReference bytes = out.bytes();
            try (StreamInput in = bytes.streamInput()) {
                IgnoredFieldStats clone = new IgnoredFieldStats(in);
                assertThat(clone.getTotalNumberOfDocuments(), equalTo(original.getTotalNumberOfDocuments()));
                assertThat(clone.getDocsWithIgnoredFields(), equalTo(original.getDocsWithIgnoredFields()));
                assertThat(clone.getIgnoredFieldTermsSumDocFreq(), equalTo(original.getIgnoredFieldTermsSumDocFreq()));
            }
        }
    }
}
