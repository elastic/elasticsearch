/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

public class FetchSearchResultTests extends ESTestCase {

    public void testSerializationRoundTripsFetchDiagnosticsOnNewVersion() throws IOException {
        FetchSearchResult original = new FetchSearchResult(new ShardSearchContextId(randomAlphaOfLength(5), 1), shardTarget());
        try {
            original.shardResult(
                new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f),
                null
            );
            original.setFetchQueueWaitMs(11L);
            original.setFetchServiceMs(22L);
            original.setResponseBytesUncompressed(333L);

            FetchSearchResult copy = copyWithVersion(original, TransportVersion.current());
            try {
                assertEquals(11L, copy.getFetchQueueWaitMs());
                assertEquals(22L, copy.getFetchServiceMs());
                assertEquals(333L, copy.getResponseBytesUncompressed());
            } finally {
                copy.decRef();
            }
        } finally {
            original.decRef();
        }
    }

    public void testSerializationOmitsFetchDiagnosticsOnOldVersion() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(FetchSearchResult.FETCH_RESULT_DIAGNOSTICS, true);
        FetchSearchResult original = new FetchSearchResult(new ShardSearchContextId(randomAlphaOfLength(5), 1), shardTarget());
        try {
            original.shardResult(
                new SearchHits(new SearchHit[] { new SearchHit(1) }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1f),
                null
            );
            original.setFetchQueueWaitMs(11L);
            original.setFetchServiceMs(22L);
            original.setResponseBytesUncompressed(333L);

            FetchSearchResult copy = copyWithVersion(original, oldVersion);
            try {
                assertEquals(-1L, copy.getFetchQueueWaitMs());
                assertEquals(-1L, copy.getFetchServiceMs());
                assertEquals(-1L, copy.getResponseBytesUncompressed());
            } finally {
                copy.decRef();
            }
        } finally {
            original.decRef();
        }
    }

    private static FetchSearchResult copyWithVersion(FetchSearchResult result, TransportVersion version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            result.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                return new FetchSearchResult(in);
            }
        }
    }

    private static SearchShardTarget shardTarget() {
        return new SearchShardTarget("node-1", new ShardId("index", "uuid", 0), null);
    }
}
