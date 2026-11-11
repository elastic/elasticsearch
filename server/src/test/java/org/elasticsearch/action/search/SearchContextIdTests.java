/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.BytesRefRecycler;

import java.io.IOException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class SearchContextIdTests extends ESTestCase {

    QueryBuilder randomQueryBuilder() {
        if (randomBoolean()) {
            return new TermQueryBuilder(randomAlphaOfLength(10), randomAlphaOfLength(10));
        } else if (randomBoolean()) {
            return new MatchAllQueryBuilder();
        } else {
            return new IdsQueryBuilder().addIds(randomAlphaOfLength(10));
        }
    }

    private static NamedWriteableRegistry queryBuilderRegistry() {
        return new NamedWriteableRegistry(
            List.of(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, IdsQueryBuilder.NAME, IdsQueryBuilder::new)
            )
        );
    }

    private Map<String, AliasFilter> randomAliasFilters() {
        return randomBoolean() ? Map.of("uuid1", AliasFilter.of(randomQueryBuilder(), "alias-" + between(1, 10))) : Map.of();
    }

    public void testEncode() {
        final NamedWriteableRegistry namedWriteableRegistry = queryBuilderRegistry();
        final AtomicArray<SearchPhaseResult> queryResults = TransportSearchHelperTests.generateQueryResults();
        final TransportVersion version = TransportVersion.current();
        final Map<String, AliasFilter> aliasFilters = new HashMap<>();
        Map<SearchShardTarget, ShardSearchFailure> shardSearchFailures = new HashMap<>();
        int idx = 0;
        for (SearchPhaseResult result : queryResults.asList()) {
            if (randomBoolean()) {
                shardSearchFailures.put(
                    result.getSearchShardTarget(),
                    new ShardSearchFailure(
                        new NoShardAvailableActionException(result.getSearchShardTarget().getShardId()),
                        result.getSearchShardTarget()
                    )
                );
                queryResults.set(idx, null);
            } else {
                final AliasFilter aliasFilter;
                if (randomBoolean()) {
                    aliasFilter = AliasFilter.of(randomQueryBuilder());
                } else if (randomBoolean()) {
                    aliasFilter = AliasFilter.of(randomQueryBuilder(), "alias-" + between(1, 10));
                } else {
                    aliasFilter = AliasFilter.EMPTY;
                }
                if (randomBoolean()) {
                    aliasFilters.put(result.getSearchShardTarget().getShardId().getIndex().getUUID(), aliasFilter);
                }
            }
            idx += 1;
        }
        final BytesReference id = SearchContextId.encode(
            queryResults.asList(),
            aliasFilters,
            version,
            shardSearchFailures.values().toArray(ShardSearchFailure[]::new)
        );
        final SearchContextId context = SearchContextId.decode(namedWriteableRegistry, id);
        assertThat(context.shards().keySet(), hasSize(3));
        // TODO assertThat(context.failedShards().keySet(), hasSize(shardsFailed));
        assertThat(context.aliasFilter(), equalTo(aliasFilters));

        ShardId shardIdForNode1 = new ShardId("idx", "uuid1", 2);
        SearchShardTarget shardTargetForNode1 = new SearchShardTarget("node_1", shardIdForNode1, "cluster_x");
        SearchContextIdForNode node1 = context.shards().get(shardIdForNode1);
        assertThat(node1.getClusterAlias(), equalTo("cluster_x"));
        if (shardSearchFailures.containsKey(shardTargetForNode1)) {
            assertNull(node1.getNode());
            assertNull(node1.getSearchContextId());
        } else {
            assertThat(node1.getNode(), equalTo("node_1"));
            assertThat(node1.getSearchContextId().getId(), equalTo(1L));
            assertThat(node1.getSearchContextId().getSessionId(), equalTo("a"));
        }

        ShardId shardIdForNode2 = new ShardId("idy", "uuid2", 42);
        SearchShardTarget shardTargetForNode2 = new SearchShardTarget("node_2", shardIdForNode2, "cluster_y");
        SearchContextIdForNode node2 = context.shards().get(shardIdForNode2);
        assertThat(node2.getClusterAlias(), equalTo("cluster_y"));
        if (shardSearchFailures.containsKey(shardTargetForNode2)) {
            assertNull(node2.getNode());
            assertNull(node2.getSearchContextId());
        } else {
            assertThat(node2.getNode(), equalTo("node_2"));
            assertThat(node2.getSearchContextId().getId(), equalTo(12L));
            assertThat(node2.getSearchContextId().getSessionId(), equalTo("b"));
        }

        ShardId shardIdForNode3 = new ShardId("idy", "uuid2", 43);
        SearchShardTarget shardTargetForNode3 = new SearchShardTarget("node_3", shardIdForNode3, null);
        SearchContextIdForNode node3 = context.shards().get(shardIdForNode3);
        assertThat(node3.getClusterAlias(), nullValue());
        if (shardSearchFailures.containsKey(shardTargetForNode3)) {
            assertNull(node3.getNode());
            assertNull(node3.getSearchContextId());
        } else {
            assertThat(node3.getNode(), equalTo("node_3"));
            assertThat(node3.getSearchContextId().getId(), equalTo(42L));
            assertThat(node3.getSearchContextId().getSessionId(), equalTo("c"));
        }

        final String[] indices = SearchContextId.decodeIndices(id);
        assertThat(indices.length, equalTo(3));
        assertThat(indices[0], equalTo("cluster_x:idx"));
        assertThat(indices[1], equalTo("cluster_y:idy"));
        assertThat(indices[2], equalTo("idy"));
    }

    public void testDecodingWithUnknownTransportIdThrows() {
        TransportVersion unknownTransportVersion = TransportVersionUtils.getNextVersion(TransportVersion.current(), true);
        BytesReference id = SearchContextId.encode(
            Collections.emptyMap(),
            Collections.emptyMap(),
            unknownTransportVersion,
            ShardSearchFailure.EMPTY_ARRAY
        );

        NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.emptyList());

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SearchContextId.decode(registry, id));
        assertThat(e.getMessage(), equalTo("unknown transport version [" + unknownTransportVersion.id() + "] reading search context id"));
    }

    public void testDecodingWithForgedOversizedAliasFilterMapThrows() {
        byte[] payload = new byte[] {
            (byte) 0xa7,
            (byte) 0xc7,
            (byte) 0xba,
            0x04,        // transport version
            0x00,        // empty shards map
            (byte) 0x80,
            (byte) 0xa8,
            (byte) 0xd6,
            (byte) 0xb9,
            0x07         // alias-filter map size ~1e9
        };
        NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.emptyList());
        expectThrows(IllegalArgumentException.class, () -> SearchContextId.decode(registry, new BytesArray(payload)));
    }

    public void testNotAllBytesRead() throws IOException {
        NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.emptyList());

        try (BytesStreamOutput output = new BytesStreamOutput()) {
            BytesReference id = SearchContextId.encode(
                Collections.emptyMap(),
                Collections.emptyMap(),
                TransportVersion.current(),
                ShardSearchFailure.EMPTY_ARRAY
            );
            id.writeTo(output);
            output.writeBoolean(true);

            BytesReference data = output.bytes();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SearchContextId.decode(registry, data));
            assertThat(e.getMessage(), equalTo("Not all bytes were read"));
        }
    }

    public void testDecodeIndicesWithUnknownTransportVersionThrows() {
        TransportVersion unknownTransportVersion = TransportVersionUtils.getNextVersion(TransportVersion.current(), true);
        BytesReference id = SearchContextId.encode(
            Collections.emptyMap(),
            Collections.emptyMap(),
            unknownTransportVersion,
            ShardSearchFailure.EMPTY_ARRAY
        );

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SearchContextId.decodeIndices(id));
        assertThat(e.getMessage(), equalTo("unknown transport version [" + unknownTransportVersion.id() + "] reading search context id"));
    }

    public void testDecodeKnownTransportVersionThenInvalid() throws IOException {
        NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.emptyList());

        // correct start, including transport version, but then invalid
        final BytesReference garbageId;
        try (var out = new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
            TransportVersion.writeVersion(TransportVersion.current(), out);
            out.writeVInt(-1);
            garbageId = out.bytes();
        }

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SearchContextId.decode(registry, garbageId));
        assertThat(e.getMessage(), equalTo("invalid search context id"));
        assertThat(
            asInstanceOf(NegativeArraySizeException.class, e.getCause()).getMessage(),
            equalTo("array size must be positive but was: -1")
        );

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> SearchContextId.decodeIndices(garbageId));
        assertThat(e2.getMessage(), equalTo("invalid search context id"));
        assertThat(
            asInstanceOf(NegativeArraySizeException.class, e2.getCause()).getMessage(),
            equalTo("array size must be positive but was: -1")
        );
    }

    public void testDecodeLegacyPitIdThrowsIllegalArgument() {
        NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.emptyList());
        // a older PIT ID, that cannot be read anymore
        String legacyPitBase64 =
            "46ToAwMDaWR5BXV1aWQyKwZub2RlXzMAAAAAAAAAACoBYwADaWR4BXV1aWQxAgZub2RlXzEAAAAAAAAAAAEBYQADaWR5BXV1aWQyKgZub2RlXzIA"
                + "AAAAAAAAAAwBYgACBXV1aWQyAAAFdXVpZDEAAQltYXRjaF9hbGw_gAAAAA==";
        BytesReference legacyId = new BytesArray(Base64.getUrlDecoder().decode(legacyPitBase64));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SearchContextId.decode(registry, legacyId));
        assertThat(e.getMessage(), startsWith("unknown transport version"));

        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> SearchContextId.decodeIndices(legacyId));
        assertThat(e2.getMessage(), startsWith("unknown transport version"));
    }

    public void testDecodeGarbageBytesThrowsIllegalArgument() {
        NamedWriteableRegistry registry = new NamedWriteableRegistry(Collections.emptyList());
        for (int length : new int[] { 0, 1, between(2, 1024) }) {
            BytesReference garbageId = new BytesArray(randomByteArrayOfLength(length));

            expectThrows(IllegalArgumentException.class, () -> SearchContextId.decode(registry, garbageId));
            expectThrows(IllegalArgumentException.class, () -> SearchContextId.decodeIndices(garbageId));
        }
    }

    public void testDecodeTruncatedValidDataThrowsIllegalArgument() {
        NamedWriteableRegistry registry = queryBuilderRegistry();
        List<SearchPhaseResult> queryResults = TransportSearchHelperTests.generateQueryResults().asList();
        TransportVersion version = TransportVersion.current();

        BytesReference validId = SearchContextId.encode(queryResults, randomAliasFilters(), version, ShardSearchFailure.EMPTY_ARRAY);
        SearchContextId.decode(registry, validId);

        // the shards section ends right before the alias filter map, whose size marker is a single byte when empty
        int shardsSectionEnd = SearchContextId.encode(queryResults, Map.of(), version, ShardSearchFailure.EMPTY_ARRAY).length() - 1;

        byte[] validBytes = BytesReference.toBytes(validId);
        // truncate bytes at random position, so due to randomization we will fail at every position over time
        int truncateAt = randomIntBetween(1, validBytes.length - 1);
        BytesReference truncatedId = new BytesArray(Arrays.copyOf(validBytes, truncateAt));
        expectThrows(IllegalArgumentException.class, () -> SearchContextId.decode(registry, truncatedId));
        // decodeIndices only reads the shards section and ignores anything after it
        if (truncateAt < shardsSectionEnd) {
            expectThrows(IllegalArgumentException.class, () -> SearchContextId.decodeIndices(truncatedId));
        } else {
            assertThat(SearchContextId.decodeIndices(truncatedId), arrayContaining("cluster_x:idx", "cluster_y:idy", "idy"));
        }
    }

    public void testDecodeValidTransportVersionWithCorruptedPayloadThrowsIllegalArgument() throws IOException {
        NamedWriteableRegistry registry = queryBuilderRegistry();
        TransportVersion version = TransportVersion.current();

        BytesReference validId = SearchContextId.encode(
            TransportSearchHelperTests.generateQueryResults().asList(),
            randomAliasFilters(),
            version,
            ShardSearchFailure.EMPTY_ARRAY
        );
        SearchContextId.decode(registry, validId);
        byte[] validBytes = BytesReference.toBytes(validId);

        final int headerLength;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            TransportVersion.writeVersion(version, out);
            headerLength = out.bytes().length();
        }

        // keep the transport version header, replace the rest with random data
        byte[] corrupted = new byte[validBytes.length];
        System.arraycopy(validBytes, 0, corrupted, 0, headerLength);
        BytesReference corruptedId = null;
        do {
            byte[] garbage = randomByteArrayOfLength(corrupted.length - headerLength);
            System.arraycopy(garbage, 0, corrupted, headerLength, garbage.length);
            corruptedId = new BytesArray(corrupted);
            // due to randomization we might end up with the same random than the original, guard against this
        } while (validId.equals(corruptedId));

        final BytesReference data = corruptedId;
        expectThrows(IllegalArgumentException.class, () -> SearchContextId.decode(registry, data));
    }
}
