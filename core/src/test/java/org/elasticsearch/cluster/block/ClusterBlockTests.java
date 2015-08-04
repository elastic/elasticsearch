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
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.EnumSet;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.equalTo;

public class ClusterBlockTests extends ESTestCase {

    @Test
    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(10, 100);
        for (int i = 0; i < iterations; i++) {
            // Get a random version
            Version version = randomVersion(random());

            // Get a random list of ClusterBlockLevels
            EnumSet<ClusterBlockLevel> levels = EnumSet.noneOf(ClusterBlockLevel.class);
            int nbLevels = randomIntBetween(1, ClusterBlockLevel.values().length);
            for (int j = 0; j < nbLevels; j++) {
                levels.add(randomFrom(ClusterBlockLevel.values()));
            }

            ClusterBlock clusterBlock = new ClusterBlock(randomInt(), "cluster block #" + randomInt(), randomBoolean(),
                    randomBoolean(), randomFrom(RestStatus.values()), levels);

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(version);
            clusterBlock.writeTo(out);

            StreamInput in = StreamInput.wrap(out.bytes());
            in.setVersion(version);
            ClusterBlock result = ClusterBlock.readClusterBlock(in);

            assertThat(result.id(), equalTo(clusterBlock.id()));
            assertThat(result.status(), equalTo(clusterBlock.status()));
            assertThat(result.description(), equalTo(clusterBlock.description()));
            assertThat(result.retryable(), equalTo(clusterBlock.retryable()));
            assertThat(result.disableStatePersistence(), equalTo(clusterBlock.disableStatePersistence()));
            assertArrayEquals(result.levels().toArray(), clusterBlock.levels().toArray());
        }
    }
}
