/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.block;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.EnumSet.copyOf;
import static org.elasticsearch.test.TransportVersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class ClusterBlockTests extends ESTestCase {

    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            TransportVersion version = randomVersion(random());
            ClusterBlock clusterBlock = randomClusterBlock();

            BytesStreamOutput out = new BytesStreamOutput();
            out.setTransportVersion(version);
            clusterBlock.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            in.setTransportVersion(version);
            ClusterBlock result = new ClusterBlock(in);

            assertClusterBlockEquals(clusterBlock, result);
        }
    }

    public void testToStringDanglingComma() {
        final ClusterBlock clusterBlock = randomClusterBlock();
        assertThat(clusterBlock.toString(), not(endsWith(",")));
    }

    public void testGlobalBlocksCheckedIfNoIndicesSpecified() {
        ClusterBlock globalBlock = randomClusterBlock();
        ClusterBlocks clusterBlocks = new ClusterBlocks(Collections.singleton(globalBlock), Map.of());
        ClusterBlockException exception = clusterBlocks.indicesBlockedException(randomFrom(globalBlock.levels()), new String[0]);
        assertNotNull(exception);
        assertEquals(exception.blocks(), Collections.singleton(globalBlock));
    }

    public void testRemoveIndexBlockWithId() {
        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        builder.addIndexBlock("index-1", new ClusterBlock(1, "uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));
        builder.addIndexBlock("index-1", new ClusterBlock(2, "uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));
        builder.addIndexBlock("index-1", new ClusterBlock(3, "uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));
        builder.addIndexBlock(
            "index-1",
            new ClusterBlock(3, "other uuid", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL))
        );

        builder.addIndexBlock("index-2", new ClusterBlock(3, "uuid3", "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL)));

        ClusterBlocks clusterBlocks = builder.build();
        assertThat(clusterBlocks.indices().get("index-1").size(), equalTo(4));
        assertThat(clusterBlocks.indices().get("index-2").size(), equalTo(1));

        builder.removeIndexBlockWithId("index-1", 3);
        clusterBlocks = builder.build();

        assertThat(clusterBlocks.indices().get("index-1").size(), equalTo(2));
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 1), is(true));
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 2), is(true));
        assertThat(clusterBlocks.indices().get("index-2").size(), equalTo(1));
        assertThat(clusterBlocks.hasIndexBlockWithId("index-2", 3), is(true));

        builder.removeIndexBlockWithId("index-2", 3);
        clusterBlocks = builder.build();

        assertThat(clusterBlocks.indices().get("index-1").size(), equalTo(2));
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 1), is(true));
        assertThat(clusterBlocks.hasIndexBlockWithId("index-1", 2), is(true));
        assertThat(clusterBlocks.indices().get("index-2"), nullValue());
        assertThat(clusterBlocks.hasIndexBlockWithId("index-2", 3), is(false));
    }

    public void testGetIndexBlockWithId() {
        final int blockId = randomInt();
        final ClusterBlock[] clusterBlocks = new ClusterBlock[randomIntBetween(1, 5)];

        final ClusterBlocks.Builder builder = ClusterBlocks.builder();
        for (int i = 0; i < clusterBlocks.length; i++) {
            clusterBlocks[i] = new ClusterBlock(blockId, "uuid" + i, "", true, true, true, RestStatus.OK, copyOf(ClusterBlockLevel.ALL));
            builder.addIndexBlock("index", clusterBlocks[i]);
        }

        assertThat(builder.build().indices().get("index").size(), equalTo(clusterBlocks.length));
        assertThat(builder.build().getIndexBlockWithId("index", blockId), is(oneOf(clusterBlocks)));
        assertThat(builder.build().getIndexBlockWithId("index", randomValueOtherThan(blockId, ESTestCase::randomInt)), nullValue());
    }

    private static ClusterBlock randomClusterBlock() {
        final String uuid = randomBoolean() ? UUIDs.randomBase64UUID() : null;
        final List<ClusterBlockLevel> levels = Arrays.asList(ClusterBlockLevel.values());
        return new ClusterBlock(
            randomInt(),
            uuid,
            "cluster block #" + randomInt(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomFrom(RestStatus.values()),
            copyOf(randomSubsetOf(randomIntBetween(1, levels.size()), levels))
        );
    }

    private void assertClusterBlockEquals(final ClusterBlock expected, final ClusterBlock actual) {
        assertEquals(expected, actual);
        assertThat(actual.id(), equalTo(expected.id()));
        assertThat(actual.uuid(), equalTo(expected.uuid()));
        assertThat(actual.status(), equalTo(expected.status()));
        assertThat(actual.description(), equalTo(expected.description()));
        assertThat(actual.retryable(), equalTo(expected.retryable()));
        assertThat(actual.disableStatePersistence(), equalTo(expected.disableStatePersistence()));
        assertArrayEquals(actual.levels().toArray(), expected.levels().toArray());
    }
}
