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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

import static org.hamcrest.Matchers.equalTo;

public class AliasAndIndexLookupTests extends ESTestCase {

    public void testAddIndexWithNoAliases() {
        String indexName = randomAlphaOfLengthBetween(5, 10);
        IndexMetaData indexMetaData = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(0).build();
        AliasAndIndexLookupBuilder builder = new AliasAndIndexLookupBuilder();
        builder.addIndex(indexMetaData);
        SortedMap<String, AliasOrIndex> lookup = builder.build();
        assertThat(lookup.size(), equalTo(1));
        assertTrue(lookup.containsKey(indexName));
        AliasOrIndex.Index index = (AliasOrIndex.Index) lookup.get(indexName);
        assertSame(index.getIndex(), indexMetaData);
    }

    public void testAddIndexWithAliases() {
        String indexName = randomAlphaOfLength(11);
        IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(0);
        int numAliases = randomIntBetween(1, 10);
        for (int i = 0; i < numAliases; i++) {
            indexMetaDataBuilder.putAlias(AliasMetaData.builder(randomAlphaOfLength(i + 1)));
        }
        IndexMetaData indexMetaData = indexMetaDataBuilder.build();

        AliasAndIndexLookupBuilder builder = new AliasAndIndexLookupBuilder();
        builder.addIndex(indexMetaData);

        SortedMap<String, AliasOrIndex> lookup = builder.build();
        assertThat(lookup.size(), equalTo(numAliases + 1));
        assertTrue(lookup.containsKey(indexName));
        AliasOrIndex.Index index = (AliasOrIndex.Index) lookup.get(indexName);
        assertSame(index.getIndex(), indexMetaData);
        for (ObjectCursor<AliasMetaData> cursor : indexMetaData.getAliases().values()) {
            AliasOrIndex.Alias alias = (AliasOrIndex.Alias) lookup.get(cursor.value.getAlias());
            assertThat(alias.getIndices().size(), equalTo(1));
            assertSame(alias.getIndices().iterator().next(), indexMetaData);
        }
    }

    public void testRemoveIndex() {
        int indexCount = randomIntBetween(10, 100);
        Set<String> indices = new HashSet<>(indexCount);
        for (int i = 0; i < indexCount; i++) {
            indices.add(randomAlphaOfLength(10));
        }
        Map<String, Set<String>> aliasToIndices = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 3); i++) {
            aliasToIndices.put(randomAlphaOfLength(5),
                new HashSet<>(randomSubsetOf(randomIntBetween(1, 3), indices)));
        }

        AliasAndIndexLookupBuilder builder = new AliasAndIndexLookupBuilder();

        List<IndexMetaData> indicesMetadatas = new ArrayList<>();
        for (String index : indices) {
            IndexMetaData.Builder indexBuilder = IndexMetaData.builder(index)
                .settings(settings(Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0);
            aliasToIndices.forEach((key, value) -> {
                if (value.contains(index)) {
                    indexBuilder.putAlias(AliasMetaData.builder(key).build());
                }
            });
            IndexMetaData indexMetaData = indexBuilder.build();
            indicesMetadatas.add(indexMetaData);
            builder.addIndex(indexMetaData);
        }
        IndexMetaData indexToRemove = randomFrom(indicesMetadatas);
        Object[] aliases = indexToRemove.getAliases().values().toArray();
        builder.removeIndex(indexToRemove);
        SortedMap<String, AliasOrIndex> lookup = builder.build();
        assertFalse(lookup.containsKey(indexToRemove.getIndex().getName()));
        for (Object aliasMetaData : aliases) {
            AliasOrIndex aliasOrIndex = lookup.get(((AliasMetaData) aliasMetaData).getAlias());
            if (aliasOrIndex != null) {
                assertFalse(aliasOrIndex.getIndices().contains(indexToRemove));
            }
        }
    }

    public void testRemoveAlias() {
        String indexName = randomAlphaOfLength(10);
        IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(indexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(1).numberOfReplicas(0);
        int numAliases = randomIntBetween(1, 3);
        List<String> aliases = new ArrayList<>(numAliases);
        for (int i = 0; i < numAliases; i++) {
            String alias = randomAlphaOfLength(5);
            aliases.add(alias);
            indexMetaDataBuilder.putAlias(AliasMetaData.builder(alias));
        }
        IndexMetaData indexMetaData = indexMetaDataBuilder.build();

        AliasAndIndexLookupBuilder builder = new AliasAndIndexLookupBuilder();
        builder.addIndex(indexMetaData);

        String removedAlias = randomFrom(aliases);
        builder.removeAlias(indexName, removedAlias);

        SortedMap<String, AliasOrIndex> lookup = builder.build();
        assertSame(lookup.get(indexName).getIndices().iterator().next(), indexMetaData);
        for (String alias : aliases) {
            if (alias.equals(removedAlias)) {
                assertFalse(lookup.containsKey(alias));
            } else {
                assertTrue(lookup.containsKey(alias));
            }
        }
    }
}
