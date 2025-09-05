/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.suggest.stats;

import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.FieldMemoryStatsTests;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class CompletionsStatsTests extends ESTestCase {

    public void testSerialize() throws IOException {
        FieldMemoryStats map = randomBoolean() ? null : FieldMemoryStatsTests.randomFieldMemoryStats();
        CompletionStats stats = new CompletionStats(randomNonNegativeLong(), map);
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        CompletionStats read = new CompletionStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getSizeInBytes(), read.getSizeInBytes());
        assertEquals(stats.getFields(), read.getFields());
    }
}
