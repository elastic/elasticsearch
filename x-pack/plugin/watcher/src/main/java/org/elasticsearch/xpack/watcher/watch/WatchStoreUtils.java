/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.watch;

import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.index.IndexNotFoundException;

public class WatchStoreUtils {

    /**
     * Method to get indexmetadata of a index, that potentially is behind an alias.
     *
     * @param name Name of the index or the alias
     * @param metaData Metadata to search for the name
     * @return IndexMetaData of the concrete index
     * @throws IllegalStateException If an alias points to two indices
     * @throws IndexNotFoundException If no index exists
     */
    public static IndexMetaData getConcreteIndex(String name, MetaData metaData) {
        AliasOrIndex aliasOrIndex = metaData.getAliasAndIndexLookup().get(name);
        if (aliasOrIndex == null) {
            return null;
        }

        if (aliasOrIndex.isAlias() && aliasOrIndex.getIndices().size() > 1) {
            throw new IllegalStateException("Alias [" + name + "] points to more than one index");
        }

        return aliasOrIndex.getIndices().get(0);
    }

}
