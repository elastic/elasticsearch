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
package org.elasticsearch.snapshots;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;

public class SnapshotUtilsTests extends ESTestCase {
    public void testIndexNameFiltering() {
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"*"}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"_all"}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"foo", "bar", "baz"}, new String[]{"foo", "bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"foo"}, new String[]{"foo"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"baz", "not_available"}, new String[]{"baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"ba*", "-bar", "-baz"}, new String[]{});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"-bar"}, new String[]{"foo", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"-ba*"}, new String[]{"foo"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"+ba*"}, new String[]{"bar", "baz"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"+bar", "+foo"}, new String[]{"bar", "foo"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"zzz", "bar"}, IndicesOptions.lenientExpandOpen(),
            new String[]{"bar"});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{""}, IndicesOptions.lenientExpandOpen(), new String[]{});
        assertIndexNameFiltering(new String[]{"foo", "bar", "baz"}, new String[]{"foo", "", "ba*"}, IndicesOptions.lenientExpandOpen(),
            new String[]{"foo", "bar", "baz"});
    }

    private void assertIndexNameFiltering(String[] indices, String[] filter, String[] expected) {
        assertIndexNameFiltering(indices, filter, IndicesOptions.lenientExpandOpen(), expected);
    }

    private void assertIndexNameFiltering(String[] indices, String[] filter, IndicesOptions indicesOptions, String[] expected) {
        List<String> indicesList = Arrays.asList(indices);
        List<String> actual = SnapshotUtils.filterIndices(indicesList, filter, indicesOptions);
        assertThat(actual, containsInAnyOrder(expected));
    }

    public void testFilterIndexIdsForClone() {
        final SnapshotId sourceSnapshotId = new SnapshotId("source", UUIDs.randomBase64UUID(random()));
        final Snapshot targetSnapshot = new Snapshot("test-repo", new SnapshotId("target", UUIDs.randomBase64UUID(random())));
        final IndexId existingIndexId = new IndexId("test-idx", UUIDs.randomBase64UUID(random()));
        final RepositoryData repositoryData = RepositoryData.EMPTY.addSnapshot(sourceSnapshotId, SnapshotState.SUCCESS, Version.CURRENT,
                ShardGenerations.builder().put(existingIndexId, 0, UUIDs.randomBase64UUID()).build(),
                Collections.emptyMap(), Collections.emptyMap());
        {
            final SnapshotException sne = expectThrows(SnapshotException.class, () -> SnapshotUtils.findIndexIdsToClone(
                    sourceSnapshotId, targetSnapshot, repositoryData, "does-not-exist"));
            assertThat(sne.getMessage(), containsString("No index [does-not-exist] found in the source snapshot "));
        }
        {
            final SnapshotException sne = expectThrows(SnapshotException.class, () -> SnapshotUtils.findIndexIdsToClone(
                    sourceSnapshotId, targetSnapshot, repositoryData, "does-not-exist-*"));
            assertThat(sne.getMessage(), containsString("No indices in the source snapshot [" + sourceSnapshotId +
                    "] matched requested pattern ["));
        }
        assertThat(SnapshotUtils.findIndexIdsToClone(sourceSnapshotId, targetSnapshot, repositoryData, existingIndexId.getName()),
                contains(existingIndexId));
        assertThat(SnapshotUtils.findIndexIdsToClone(sourceSnapshotId, targetSnapshot, repositoryData, "test-*"),
                contains(existingIndexId));
    }
}
