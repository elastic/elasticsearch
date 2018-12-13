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

import org.elasticsearch.Version;
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
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetaDataIndexAliasesServiceTests extends ESTestCase {
    private final AliasValidator aliasValidator = new AliasValidator();
    private final MetaDataDeleteIndexService deleteIndexService = mock(MetaDataDeleteIndexService.class);
    private final MetaDataIndexAliasesService service = new MetaDataIndexAliasesService(null, null, aliasValidator,
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
        ClusterState after = service.innerExecute(before, singletonList(new AliasAction.Add(index, "test", null, null, null, null)));
        AliasOrIndex alias = after.metaData().getAliasAndIndexLookup().get("test");
        assertNotNull(alias);
        assertTrue(alias.isAlias());
        assertThat(alias.getIndices(), contains(after.metaData().index(index)));

        // Remove the alias from it while adding another one
        before = after;
        after = service.innerExecute(before, Arrays.asList(
                new AliasAction.Remove(index, "test"),
                new AliasAction.Add(index, "test_2", null, null, null, null)));
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
                new AliasAction.Add("test_2", "test", null, null, null, null),
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
                new AliasAction.Add("test", "alias", null, null, null, null),
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

    public void testAddWriteOnlyWithNoExistingAliases() {
        ClusterState before = createIndex(ClusterState.builder(ClusterName.DEFAULT).build(), "test");

        ClusterState after = service.innerExecute(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, false)));
        assertFalse(after.metaData().index("test").getAliases().get("alias").writeIndex());
        assertNull(((AliasOrIndex.Alias) after.metaData().getAliasAndIndexLookup().get("alias")).getWriteIndex());

        after = service.innerExecute(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, null)));
        assertNull(after.metaData().index("test").getAliases().get("alias").writeIndex());
        assertThat(((AliasOrIndex.Alias) after.metaData().getAliasAndIndexLookup().get("alias")).getWriteIndex(),
            equalTo(after.metaData().index("test")));

        after = service.innerExecute(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, true)));
        assertTrue(after.metaData().index("test").getAliases().get("alias").writeIndex());
        assertThat(((AliasOrIndex.Alias) after.metaData().getAliasAndIndexLookup().get("alias")).getWriteIndex(),
            equalTo(after.metaData().index("test")));
    }

    public void testAddWriteOnlyWithExistingWriteIndex() {
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder("test")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetaData.Builder indexMetaData2 = IndexMetaData.builder("test2")
            .putAlias(AliasMetaData.builder("alias").writeIndex(true).build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetaData).put(indexMetaData2)).build();

        ClusterState after = service.innerExecute(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, null)));
        assertNull(after.metaData().index("test").getAliases().get("alias").writeIndex());
        assertThat(((AliasOrIndex.Alias) after.metaData().getAliasAndIndexLookup().get("alias")).getWriteIndex(),
            equalTo(after.metaData().index("test2")));

        Exception exception = expectThrows(IllegalStateException.class, () -> service.innerExecute(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, true))));
        assertThat(exception.getMessage(), startsWith("alias [alias] has more than one write index ["));
    }

    public void testSwapWriteOnlyIndex() {
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder("test")
            .putAlias(AliasMetaData.builder("alias").writeIndex(true).build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetaData.Builder indexMetaData2 = IndexMetaData.builder("test2")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetaData).put(indexMetaData2)).build();

        Boolean unsetValue = randomBoolean() ? null : false;
        List<AliasAction> swapActions = Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, unsetValue),
            new AliasAction.Add("test2", "alias", null, null, null, true)
        );
        Collections.shuffle(swapActions, random());
        ClusterState after = service.innerExecute(before, swapActions);
        assertThat(after.metaData().index("test").getAliases().get("alias").writeIndex(), equalTo(unsetValue));
        assertTrue(after.metaData().index("test2").getAliases().get("alias").writeIndex());
        assertThat(((AliasOrIndex.Alias) after.metaData().getAliasAndIndexLookup().get("alias")).getWriteIndex(),
            equalTo(after.metaData().index("test2")));
    }

    public void testAddWriteOnlyWithExistingNonWriteIndices() {
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder("test")
            .putAlias(AliasMetaData.builder("alias").writeIndex(randomBoolean() ? null : false).build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetaData.Builder indexMetaData2 = IndexMetaData.builder("test2")
            .putAlias(AliasMetaData.builder("alias").writeIndex(randomBoolean() ? null : false).build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetaData.Builder indexMetaData3 = IndexMetaData.builder("test3")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetaData).put(indexMetaData2).put(indexMetaData3)).build();

        assertNull(((AliasOrIndex.Alias) before.metaData().getAliasAndIndexLookup().get("alias")).getWriteIndex());

        ClusterState after = service.innerExecute(before, Arrays.asList(
            new AliasAction.Add("test3", "alias", null, null, null, true)));
        assertTrue(after.metaData().index("test3").getAliases().get("alias").writeIndex());
        assertThat(((AliasOrIndex.Alias) after.metaData().getAliasAndIndexLookup().get("alias")).getWriteIndex(),
            equalTo(after.metaData().index("test3")));

    }

    public void testAddWriteOnlyWithIndexRemoved() {
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder("test")
            .putAlias(AliasMetaData.builder("alias").build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetaData.Builder indexMetaData2 = IndexMetaData.builder("test2")
            .putAlias(AliasMetaData.builder("alias").build())
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetaData).put(indexMetaData2)).build();

        assertNull(before.metaData().index("test").getAliases().get("alias").writeIndex());
        assertNull(before.metaData().index("test2").getAliases().get("alias").writeIndex());
        assertNull(((AliasOrIndex.Alias) before.metaData().getAliasAndIndexLookup().get("alias")).getWriteIndex());

        ClusterState after = service.innerExecute(before, Collections.singletonList(new AliasAction.RemoveIndex("test")));
        assertNull(after.metaData().index("test2").getAliases().get("alias").writeIndex());
        assertThat(((AliasOrIndex.Alias) after.metaData().getAliasAndIndexLookup().get("alias")).getWriteIndex(),
            equalTo(after.metaData().index("test2")));
    }

    public void testAddWriteOnlyValidatesAgainstMetaDataBuilder() {
        IndexMetaData.Builder indexMetaData = IndexMetaData.builder("test")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        IndexMetaData.Builder indexMetaData2 = IndexMetaData.builder("test2")
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1);
        ClusterState before = ClusterState.builder(ClusterName.DEFAULT)
            .metaData(MetaData.builder().put(indexMetaData).put(indexMetaData2)).build();

        Exception exception = expectThrows(IllegalStateException.class, () -> service.innerExecute(before, Arrays.asList(
            new AliasAction.Add("test", "alias", null, null, null, true),
            new AliasAction.Add("test2", "alias", null, null, null, true)
        )));
        assertThat(exception.getMessage(), startsWith("alias [alias] has more than one write index ["));
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
