/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.sameInstance;

public class FetchPhaseResponseChunkTests extends ESTestCase {

    private static final ShardId TEST_SHARD_ID = new ShardId(new Index("test-index", "test-uuid"), 0);

    public void testToReleasableBytesReferenceTransfersOwnership() throws IOException {
        SearchHit hit = createHit(1);
        try {
            AtomicBoolean released = new AtomicBoolean(false);
            ReleasableBytesReference serializedHits = new ReleasableBytesReference(serializeHits(hit), () -> released.set(true));

            FetchPhaseResponseChunk chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, serializedHits, 1, 10, 0L);
            try {
                assertThat(chunk.getBytesLength(), greaterThan(0L));

                ReleasableBytesReference wireBytes = chunk.toReleasableBytesReference(42L);
                try {
                    assertThat(chunk.getBytesLength(), equalTo(0L));
                    assertFalse(released.get());

                    try (StreamInput in = wireBytes.streamInput()) {
                        assertThat(in.readVLong(), equalTo(42L));
                        FetchPhaseResponseChunk decoded = new FetchPhaseResponseChunk(in);
                        try {
                            assertThat(decoded.shardId(), equalTo(TEST_SHARD_ID));
                            assertThat(decoded.hitCount(), equalTo(1));
                            assertThat(getIdFromSource(decoded.getHits()[0]), equalTo(1));
                        } finally {
                            decoded.close();
                        }
                    }
                } finally {
                    wireBytes.decRef();
                }

                assertTrue(released.get());
            } finally {
                chunk.close();
            }
        } finally {
            hit.decRef();
        }
    }

    public void testGetHitsCachesDeserializedHits() throws IOException {
        SearchHit first = createHit(1);
        SearchHit second = createHit(2);
        try {
            FetchPhaseResponseChunk chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(first, second), 2, 10, 0L);
            try {
                SearchHit[] firstRead = chunk.getHits();
                SearchHit[] secondRead = chunk.getHits();
                assertThat(secondRead, sameInstance(firstRead));
                assertThat(secondRead.length, equalTo(2));
                assertThat(getIdFromSource(secondRead[0]), equalTo(1));
                assertThat(getIdFromSource(secondRead[1]), equalTo(2));
            } finally {
                chunk.close();
            }
        } finally {
            first.decRef();
            second.decRef();
        }
    }

    public void testGetHitsReturnsEmptyWhenHitCountIsZero() throws IOException {
        FetchPhaseResponseChunk chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, BytesArray.EMPTY, 0, 0, 0L);
        try {
            assertThat(chunk.getHits().length, equalTo(0));
        } finally {
            chunk.close();
        }
    }

    public void testCloseClearsChunkState() throws IOException {
        SearchHit hit = createHit(7);
        try {
            FetchPhaseResponseChunk chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(hit), 1, 1, 0L);

            SearchHit[] hits = chunk.getHits();
            assertTrue(hits[0].hasReferences());

            chunk.close();
            assertThat(chunk.getBytesLength(), equalTo(0L));
            assertThat(chunk.getHits().length, equalTo(0));
        } finally {
            hit.decRef();
        }
    }

    public void testSerializationRoundTripAcrossCompatibleTransportVersion() throws IOException {
        SearchHit hit = createHit(42);
        try {
            FetchPhaseResponseChunk chunk = new FetchPhaseResponseChunk(TEST_SHARD_ID, serializeHits(hit), 1, 1, 0L);
            try {
                TransportVersion version = randomBoolean() ? TransportVersion.current() : TransportVersionUtils.randomCompatibleVersion();
                FetchPhaseResponseChunk roundTripped = copyWriteable(
                    chunk,
                    new NamedWriteableRegistry(Collections.emptyList()),
                    FetchPhaseResponseChunk::new,
                    version
                );
                try {
                    assertThat(roundTripped.shardId(), equalTo(TEST_SHARD_ID));
                    assertThat(roundTripped.hitCount(), equalTo(1));
                    assertThat(roundTripped.expectedTotalDocs(), equalTo(1));
                    assertThat(roundTripped.sequenceStart(), equalTo(0L));
                    assertThat(getIdFromSource(roundTripped.getHits()[0]), equalTo(42));
                } finally {
                    roundTripped.close();
                }
            } finally {
                chunk.close();
            }
        } finally {
            hit.decRef();
        }
    }

    private SearchHit createHit(int id) {
        SearchHit hit = new SearchHit(id);
        hit.sourceRef(new BytesArray("{\"id\":" + id + "}"));
        return hit;
    }

    private BytesReference serializeHits(SearchHit... hits) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            for (SearchHit hit : hits) {
                hit.writeTo(out);
            }
            return out.bytes();
        }
    }

    private int getIdFromSource(SearchHit hit) {
        Number id = (Number) XContentHelper.convertToMap(hit.getSourceRef(), false, XContentType.JSON).v2().get("id");
        return id.intValue();
    }
}
