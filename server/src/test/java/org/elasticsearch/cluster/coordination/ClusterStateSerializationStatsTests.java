/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 * Unit tests for {@link ClusterStateSerializationStats}.
 */
public class ClusterStateSerializationStatsTests extends ESTestCase {

    /**
     * Verifies that {@link ClusterStateSerializationStats#EMPTY} has all six fields equal to zero.
     */
    public void testEmptyConstant() {
        ClusterStateSerializationStats empty = ClusterStateSerializationStats.EMPTY;
        assertEquals(0L, empty.getFullStateCount());
        assertEquals(0L, empty.getTotalUncompressedFullStateBytes());
        assertEquals(0L, empty.getTotalCompressedFullStateBytes());
        assertEquals(0L, empty.getDiffCount());
        assertEquals(0L, empty.getTotalUncompressedDiffBytes());
        assertEquals(0L, empty.getTotalCompressedDiffBytes());
    }

    /**
     * Verifies that each getter returns the value passed to the constructor.
     */
    public void testGetters() {
        long fullStateCount = randomNonNegativeLong();
        long totalUncompressedFullStateBytes = randomNonNegativeLong();
        long totalCompressedFullStateBytes = randomNonNegativeLong();
        long diffCount = randomNonNegativeLong();
        long totalUncompressedDiffBytes = randomNonNegativeLong();
        long totalCompressedDiffBytes = randomNonNegativeLong();

        ClusterStateSerializationStats stats = new ClusterStateSerializationStats(
            fullStateCount,
            totalUncompressedFullStateBytes,
            totalCompressedFullStateBytes,
            diffCount,
            totalUncompressedDiffBytes,
            totalCompressedDiffBytes
        );

        assertEquals(fullStateCount, stats.getFullStateCount());
        assertEquals(totalUncompressedFullStateBytes, stats.getTotalUncompressedFullStateBytes());
        assertEquals(totalCompressedFullStateBytes, stats.getTotalCompressedFullStateBytes());
        assertEquals(diffCount, stats.getDiffCount());
        assertEquals(totalUncompressedDiffBytes, stats.getTotalUncompressedDiffBytes());
        assertEquals(totalCompressedDiffBytes, stats.getTotalCompressedDiffBytes());
    }

    /**
     * Verifies that {@link ClusterStateSerializationStats#toXContent} produces a JSON structure with
     * {@code full_states} and {@code diffs} objects containing count and size fields.
     */
    public void testToXContent() throws IOException {
        long fullStateCount = randomNonNegativeLong();
        long totalUncompressedFullStateBytes = randomNonNegativeLong();
        long totalCompressedFullStateBytes = randomNonNegativeLong();
        long diffCount = randomNonNegativeLong();
        long totalUncompressedDiffBytes = randomNonNegativeLong();
        long totalCompressedDiffBytes = randomNonNegativeLong();

        ClusterStateSerializationStats stats = new ClusterStateSerializationStats(
            fullStateCount,
            totalUncompressedFullStateBytes,
            totalCompressedFullStateBytes,
            diffCount,
            totalUncompressedDiffBytes,
            totalCompressedDiffBytes
        );

        XContentBuilder builder = JsonXContent.contentBuilder();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = Strings.toString(builder);

        assertThat(json, containsString("\"full_states\""));
        assertThat(json, containsString("\"diffs\""));
        assertThat(json, containsString("\"count\":" + fullStateCount));
        assertThat(json, containsString("\"count\":" + diffCount));
        assertThat(json, containsString("\"uncompressed_size_in_bytes\":" + totalUncompressedFullStateBytes));
        assertThat(json, containsString("\"compressed_size_in_bytes\":" + totalCompressedFullStateBytes));
        assertThat(json, containsString("\"uncompressed_size_in_bytes\":" + totalUncompressedDiffBytes));
        assertThat(json, containsString("\"compressed_size_in_bytes\":" + totalCompressedDiffBytes));
    }

    /**
     * Verifies that {@link ClusterStateSerializationStats#EMPTY} serializes to XContent with zero counts and sizes.
     */
    public void testEmptyToXContent() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        ClusterStateSerializationStats.EMPTY.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = Strings.toString(builder);

        assertThat(json, containsString("\"full_states\""));
        assertThat(json, containsString("\"count\":0"));
        assertThat(json, containsString("\"diffs\""));
    }
}
