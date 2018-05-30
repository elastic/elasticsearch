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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaDataIndexAliasesServiceTests extends ESTestCase {
    private final AliasValidator aliasValidator = new AliasValidator(Settings.EMPTY);
    private final MetaDataDeleteIndexService deleteIndexService = mock(MetaDataDeleteIndexService.class);
    private final MetaDataIndexAliasesService service = new MetaDataIndexAliasesService(Settings.EMPTY, null, null, aliasValidator,
            deleteIndexService, xContentRegistry());

    public MetaDataIndexAliasesServiceTests() {
        // Mock any deletes so we don't need to worry about how MetaDataDeleteIndexService does its job
        when(deleteIndexService.deleteIndices(any(ClusterState.class), anySetOf(Index.class))).then(i -> {
            ClusterState state = (ClusterState) i.getArguments()[0];
            @SuppressWarnings("unchecked")
            Collection<Index> indices = (Collection<Index>) i.getArguments()[1];
            MetaData.Builder meta = MetaData.builder(state.metaData());
            for (Index index : indices) {
                assertTrue("index now found", state.metaData().hasConcreteIndex(index.getName()));
                meta.remove(index.getName()); // We only think about metadata for this test. Not routing or any other fun stuff.
            }
            return ClusterState.builder(state).metaData(meta).build();
        });
    }

    public void testAddAndRemove() {
        // Create a state with a single index
        String index = randomAlphaOfLength(5);
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), index);

        // Add an alias to it
        ClusterState after = service.innerExecute(before, singletonList(new AliasAction.Add(index, "test", null, null, null, false)));
        AliasOrIndex alias = after.metaData().getAliasAndIndexLookup().get("test");
        assertNotNull(alias);
        assertTrue(alias.isAlias());
        assertThat(alias.getIndices(), contains(after.metaData().index(index)));

        // Remove the alias from it while adding another one
        before = after;
        after = service.innerExecute(before, Arrays.asList(
                new AliasAction.Remove(index, "test"),
                new AliasAction.Add(index, "test_2", null, null, null, false)));
        assertNull(after.metaData().getAliasAndIndexLookup().get("test"));
        alias = after.metaData().getAliasAndIndexLookup().get("test_2");
        assertNotNull(alias);
        assertTrue(alias.isAlias());
        assertThat(alias.getIndices(), contains(after.metaData().index(index)));

        // Now just remove on its own
        before = after;
        after = service.innerExecute(before, singletonList(new AliasAction.Remove(index, "test_2")));
        assertNull(after.metaData().getAliasAndIndexLookup().get("test"));
        assertNull(after.metaData().getAliasAndIndexLookup().get("test_2"));
    }

    public void testSwapIndexWithAlias() {
        // Create "test" and "test_2"
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), "test");
        before = createIndex(before, "test_2");

        // Now remove "test" and add an alias to "test" to "test_2" in one go
        ClusterState after = service.innerExecute(before, Arrays.asList(
                new AliasAction.Add("test_2", "test", null, null, null, false),
                new AliasAction.RemoveIndex("test")));
        AliasOrIndex alias = after.metaData().getAliasAndIndexLookup().get("test");
        assertNotNull(alias);
        assertTrue(alias.isAlias());
        assertThat(alias.getIndices(), contains(after.metaData().index("test_2")));
    }

    public void testAddAliasToRemovedIndex() {
        // Create "test"
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), "test");

        // Attempt to add an alias to "test" at the same time as we remove it
        IndexNotFoundException e = expectThrows(IndexNotFoundException.class, () -> service.innerExecute(before, Arrays.asList(
                new AliasAction.Add("test", "alias", null, null, null, false),
                new AliasAction.RemoveIndex("test"))));
        assertEquals("test", e.getIndex().getName());
    }

    public void testRemoveIndexTwice() {
        // Create "test"
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), "test");

        // Try to remove an index twice. This should just remove the index once....
        ClusterState after = service.innerExecute(before, Arrays.asList(
                new AliasAction.RemoveIndex("test"),
                new AliasAction.RemoveIndex("test")));
        assertNull(after.metaData().getAliasAndIndexLookup().get("test"));
    }

    public void testWriteIndexDefaultAddAlias() {
        // Create "test"
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), "test");

        ClusterState after = service.innerExecute(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, null)));

        AliasOrIndex.Alias alias = (AliasOrIndex.Alias) after.metaData().getAliasAndIndexLookup().get("alias");
        assertTrue(alias.getFirstAliasMetaData().isWriteIndex());
    }

    public void testInvalidAddAliasWithWrite() {
        IndexMetaData indexFoo = IndexMetaData.builder("foo")
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putAlias(AliasMetaData.builder("my_alias").writeIndex(true))
            .build();
        IndexMetaData indexBar = IndexMetaData.builder("bar")
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putAlias(AliasMetaData.builder("my_alias").writeIndex(false))
            .build();
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexFoo, false).put(indexBar, false)).build();
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> service.innerExecute(before, Arrays.asList(
            new AliasAction.Add("bar", "my_alias", null, null, null, true))));
        assertThat(exception.getMessage(), startsWith("aliases cannot have multiple write indices"));
    }

    public void testSwapWriteIndex() {
        IndexMetaData indexFoo = IndexMetaData.builder("foo")
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putAlias(AliasMetaData.builder("my_alias").writeIndex(true))
            .build();
        IndexMetaData indexBar = IndexMetaData.builder("bar")
            .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .putAlias(AliasMetaData.builder("my_alias").writeIndex(false))
            .build();
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexFoo, false).put(indexBar, false)).build();
        List<AliasAction> actions = Arrays.asList(
            new AliasAction.Add("foo", "my_alias", null, null, null, false),
            new AliasAction.Add("bar", "my_alias", null, null, null, true));
        // order should not matter since validation occurs when MetaData is being built, after all the actions have been applied
        Collections.shuffle(actions, random());
        ClusterState after = service.innerExecute(before, actions);
        AliasOrIndex.Alias alias = (AliasOrIndex.Alias) after.metaData().getAliasAndIndexLookup().get("my_alias");
        assertThat(alias.getWriteIndices().size(), equalTo(1));
        assertThat(alias.getWriteIndices().get(0).getIndex(), equalTo(indexBar.getIndex()));
    }


    private ClusterState createIndex(ClusterState state, String index) {
        IndexMetaData indexMetaData = IndexMetaData.builder(index)
                .settings(Settings.builder().put("index.version.created", VersionUtils.randomVersion(random())))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
        return ClusterState.builder(state)
                .metaData(MetaData.builder(state.metaData()).put(indexMetaData, false))
                .build();
    }
}
