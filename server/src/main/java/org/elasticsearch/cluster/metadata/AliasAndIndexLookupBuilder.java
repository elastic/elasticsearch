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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Helper class to help build the <code>aliasAndIndexLookup</code> {@link SortedMap}
 * that is used by several services to retrieve information relating to the relationship
 * between aliases and indices.
 *
 * The {@link SortedMap} holds key-value pairs where the key is either an index name, or
 * an alias name. Index keys have values of {@link AliasOrIndex.Index}, while alias keys
 * have values of {@link AliasOrIndex.Alias}. Both {@link AliasOrIndex} types hold a
 * list of indices that they point to.
 */
public class AliasAndIndexLookupBuilder {

    private final SortedMap<String, AliasOrIndex> map;

    public AliasAndIndexLookupBuilder() {
        this.map = new TreeMap<>();
    }

    public AliasAndIndexLookupBuilder(SortedMap<String, AliasOrIndex> aliasAndIndexLookup) {
        this.map = new TreeMap<>(aliasAndIndexLookup);
    }

    public AliasOrIndex get(String aliasOrIndex) {
        return map.get(aliasOrIndex);
    }

    public boolean containsKey(String aliasOrIndex) {
        return map.containsKey(aliasOrIndex);
    }

    /**
     * This method adds an entry for the index represented by <code>indexMetaData</code>, as well
     * as an entry for each alias that points to it.
     *
     * @param indexMetaData the index to add
     * @throws IllegalStateException if an alias with the same name as an existing index is to be added.
     */
    public void addIndex(IndexMetaData indexMetaData) {
        map.put(indexMetaData.getIndex().getName(), new AliasOrIndex.Index(indexMetaData));
        for (ObjectObjectCursor<String, AliasMetaData> aliasCursor : indexMetaData.getAliases()) {
            AliasMetaData aliasMetaData = aliasCursor.value;
            map.compute(aliasMetaData.getAlias(), (aliasName, alias) -> {
                if (alias == null) {
                    return new AliasOrIndex.Alias(aliasMetaData, indexMetaData);
                } else if (alias instanceof AliasOrIndex.Alias) {
                    ((AliasOrIndex.Alias) alias).addIndex(indexMetaData);
                    return alias;
                } else {
                    String indexName = ((AliasOrIndex.Index) alias).getIndex().getIndex().getName();
                    throw new IllegalStateException("index and alias names need to be unique, but the following duplicate was found ["
                        + aliasName + " (alias of [" + indexName + "])]");
                }
            });
        }
    }

    /**
     * Removes the index and its associated aliases from the inner map. The entry
     * for the index associated with <code>indexMetaData</code> is removed, as well as
     * any references to the index in any of its aliases. Each of the indices aliases
     * will have the reference to this index in {@link AliasOrIndex.Alias#referenceIndexMetaDatas} removed.
     *
     * If the resulting {@link AliasOrIndex.Alias#referenceIndexMetaDatas} is empty, then the entry for
     * the alias is removed from the lookup map.
     *
     * @param indexMetaData The index to remove
     */
    public void removeIndex(IndexMetaData indexMetaData) {
        map.remove(indexMetaData.getIndex().getName());
        for (ObjectObjectCursor<String, AliasMetaData> aliasCursor : indexMetaData.getAliases()) {
            AliasMetaData aliasMetaData = aliasCursor.value;
            map.compute(aliasMetaData.getAlias(), (aliasName, alias) -> {
                if (alias != null && (alias instanceof AliasOrIndex.Alias)) {
                    ((AliasOrIndex.Alias) alias).removeIndex(indexMetaData);
                    return alias;
                }
                return null;
            });
            AliasOrIndex aliasOrIndex = map.get(aliasMetaData.getAlias());
            if (aliasOrIndex == null || aliasOrIndex.getIndices().isEmpty()) {
                map.remove(aliasMetaData.getAlias());
            }
        }
    }

    /**
     * Removes the reference between <code>alias</code> and <code>index</code> from the
     * lookup map. This means that the reference of the index in {@link AliasOrIndex.Alias#referenceIndexMetaDatas}
     * is removed.
     *
     * If the resulting {@link AliasOrIndex.Alias#referenceIndexMetaDatas} is empty, then the entry for
     * the alias is removed from the lookup map.
     *
     * @param index the index referenced by <code>alias</code>
     * @param alias the alias to remove
     */
    public void removeAlias(String index, String alias) {
        map.compute(alias, (keyAliasName, valAlias) -> {
            if (valAlias != null && (valAlias instanceof AliasOrIndex.Alias)) {
                AliasOrIndex.Alias aliasAlias = (AliasOrIndex.Alias) valAlias;
                for (IndexMetaData indexMetaData : aliasAlias.getIndices()) {
                    if (indexMetaData.getIndex().getName().equals(index)) {
                        aliasAlias.removeIndex(indexMetaData);
                        return aliasAlias;
                    }
                }
            }
            return null;
        });
        AliasOrIndex aliasOrIndex = map.get(alias);
        if (aliasOrIndex == null || aliasOrIndex.getIndices().isEmpty()) {
            map.remove(alias);
        }
    }

    public SortedMap<String, AliasOrIndex> build() {
        return Collections.unmodifiableSortedMap(map);
    }
}
