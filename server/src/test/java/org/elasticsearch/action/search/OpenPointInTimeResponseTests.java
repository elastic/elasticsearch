/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Base64;
import java.util.Locale;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class OpenPointInTimeResponseTests extends ESTestCase {

    public void testIdCantBeNull() {
        BytesReference pointInTimeId = null;
        expectThrows(
            NullPointerException.class,
            () -> { new OpenPointInTimeResponse(pointInTimeId, 11, 8, 2, 1, SearchResponse.Clusters.EMPTY); }
        );
    }

    public void testToXContent() throws IOException {
        String id = "test-id";
        BytesReference pointInTimeId = new BytesArray(id);

        BytesReference actual;
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            OpenPointInTimeResponse response = new OpenPointInTimeResponse(pointInTimeId, 11, 8, 2, 1, SearchResponse.Clusters.EMPTY);
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            actual = BytesReference.bytes(builder);
        }

        String encodedId = Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(pointInTimeId));
        BytesReference expected = new BytesArray(String.format(Locale.ROOT, """
            {
              "id": "%s",
              "_shards": {
                "total": 11,
                "successful": 8,
                "failed": 2,
                "skipped": 1
              }
            }
            """, encodedId));
        assertToXContentEquivalent(expected, actual, XContentType.JSON);
    }

    public void testWriteTo() throws IOException {
        BytesReference pointInTimeId = new BytesArray("test-id");
        int total = randomIntBetween(1, 5);
        int successful = randomIntBetween(0, total);
        int skipped = total - successful;  // Clusters(int,int,int) asserts skipped == total - successful
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(total, successful, skipped);
        OpenPointInTimeResponse original = new OpenPointInTimeResponse(pointInTimeId, 11, 8, 2, 1, clusters);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        var in = out.bytes().streamInput();
        OpenPointInTimeResponse copy = new OpenPointInTimeResponse(
            in.readBytesReference(),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            in.readVInt(),
            new SearchResponse.Clusters(in)
        );

        assertEquals(original.getPointInTimeId(), copy.getPointInTimeId());
        assertEquals(original.getTotalShards(), copy.getTotalShards());
        assertEquals(original.getSuccessfulShards(), copy.getSuccessfulShards());
        assertEquals(original.getFailedShards(), copy.getFailedShards());
        assertEquals(original.getSkippedShards(), copy.getSkippedShards());
        assertEquals(original.getClusters().getTotal(), copy.getClusters().getTotal());
        assertEquals(
            original.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL),
            copy.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SUCCESSFUL)
        );
        assertEquals(
            original.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED),
            copy.getClusters().getClusterStateCount(SearchResponse.Cluster.Status.SKIPPED)
        );
    }
}
