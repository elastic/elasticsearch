/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.block;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.elasticsearch.test.VersionUtils.getPreviousVersion;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

public class ClusterBlockTests extends ESTestCase {

    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(5, 20);
        for (int i = 0; i < iterations; i++) {
            Version version = randomVersion(random());
            ClusterBlock clusterBlock = randomClusterBlock(version);

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(version);
            clusterBlock.writeTo(out);

            StreamInput in = out.bytes().streamInput();
            in.setVersion(version);
            ClusterBlock result = ClusterBlock.readClusterBlock(in);

            assertClusterBlockEquals(clusterBlock, result);
        }
    }

    public void testBwcSerialization() throws Exception {
        for (int runs = 0; runs < randomIntBetween(5, 20); runs++) {
            // Generate a random cluster block in version < 7.0.0
            final Version version = randomVersionBetween(random(), Version.V_6_0_0, getPreviousVersion(Version.V_7_0_0));
            final ClusterBlock expected = randomClusterBlock(version);
            assertNull(expected.uuid());

            // Serialize to node in current version
            final BytesStreamOutput out = new BytesStreamOutput();
            expected.writeTo(out);

            // Deserialize and check the cluster block
            final ClusterBlock actual = ClusterBlock.readClusterBlock(out.bytes().streamInput());
            assertClusterBlockEquals(expected, actual);
        }

        for (int runs = 0; runs < randomIntBetween(5, 20); runs++) {
            // Generate a random cluster block in current version
            final ClusterBlock expected = randomClusterBlock(Version.CURRENT);

            // Serialize to node in version < 7.0.0
            final BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(randomVersionBetween(random(), Version.V_6_0_0, getPreviousVersion(Version.V_7_0_0)));
            expected.writeTo(out);

            // Deserialize and check the cluster block
            final StreamInput in = out.bytes().streamInput();
            in.setVersion(out.getVersion());
            final ClusterBlock actual = ClusterBlock.readClusterBlock(in);

            assertThat(actual.id(), equalTo(expected.id()));
            assertThat(actual.status(), equalTo(expected.status()));
            assertThat(actual.description(), equalTo(expected.description()));
            assertThat(actual.retryable(), equalTo(expected.retryable()));
            assertThat(actual.disableStatePersistence(), equalTo(expected.disableStatePersistence()));
            assertArrayEquals(actual.levels().toArray(), expected.levels().toArray());
        }
    }

    public void testToStringDanglingComma() {
        final ClusterBlock clusterBlock = randomClusterBlock();
        assertThat(clusterBlock.toString(), not(endsWith(",")));
    }

    public void testGlobalBlocksCheckedIfNoIndicesSpecified() {
        ClusterBlock globalBlock = randomClusterBlock();
        ClusterBlocks clusterBlocks = new ClusterBlocks(Collections.singleton(globalBlock), ImmutableOpenMap.of());
        ClusterBlockException exception = clusterBlocks.indicesBlockedException(randomFrom(globalBlock.levels()), new String[0]);
        assertNotNull(exception);
        assertEquals(exception.blocks(), Collections.singleton(globalBlock));
    }

    private ClusterBlock randomClusterBlock() {
        return randomClusterBlock(randomVersion(random()));
    }

    private ClusterBlock randomClusterBlock(final Version version) {
        final String uuid = (version.onOrAfter(Version.V_7_0_0) && randomBoolean()) ? UUIDs.randomBase64UUID() : null;
        final List<ClusterBlockLevel> levels = Arrays.asList(ClusterBlockLevel.values());
        return new ClusterBlock(randomInt(), uuid, "cluster block #" + randomInt(), randomBoolean(), randomBoolean(), randomBoolean(),
            randomFrom(RestStatus.values()), EnumSet.copyOf(randomSubsetOf(randomIntBetween(1, levels.size()), levels)));
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
