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

package org.elasticsearch.repositories;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class RepositoriesStateTests extends ESTestCase {

    public void testEqualsAndHashCode() {
        final RepositoriesState repositoriesState1 = generateRandomRepositoriesState();
        final RepositoriesState repositoriesState2 = RepositoriesState.builder().putAll(repositoriesState1).build();
        assertEquals(repositoriesState1, repositoriesState2);
        assertEquals(repositoriesState1.hashCode(), repositoriesState2.hashCode());
    }

    public void testSerializationRoundTrip() throws IOException {
        final RepositoriesState repositoriesState1 = generateRandomRepositoriesState();
        final BytesStreamOutput out = new BytesStreamOutput();
        repositoriesState1.writeTo(out);
        final RepositoriesState repositoriesState2 = new RepositoriesState(out.bytes().streamInput());
        assertEquals(repositoriesState1, repositoriesState2);
        assertEquals(repositoriesState1.hashCode(), repositoriesState2.hashCode());
    }

    private static RepositoriesState generateRandomRepositoriesState() {
        final int repoCount = randomIntBetween(0, 10);
        final RepositoriesState.Builder builder = RepositoriesState.builder();
        for (int i = 0; i < repoCount; i++) {
            final String repoName = randomAlphaOfLength(10);
            final long gen = randomLongBetween(RepositoryData.EMPTY_REPO_GEN, Integer.MAX_VALUE);
            builder.putState(repoName, gen, gen + randomLongBetween(0, Integer.MAX_VALUE));
        }
        return builder.build();
    }
}
