/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.watch;

import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.Locale;

public class WatchStoreUtils {

    /**
     * Method to get indexmetadata of a index, that potentially is behind an alias or data stream.
     *
     * @param name Name of the index or the alias
     * @param metadata Metadata to search for the name
     * @return IndexMetadata of the concrete index. If this alias or data stream has a writable index, this one is returned
     * @throws IllegalStateException If an alias points to two indices
     * @throws IndexNotFoundException If no index exists
     */
    public static IndexMetadata getConcreteIndex(String name, Metadata metadata) {
        IndexAbstraction indexAbstraction = metadata.getProject().getIndicesLookup().get(name);
        if (indexAbstraction == null) {
            return null;
        }

        if (indexAbstraction.getType() == IndexAbstraction.Type.ALIAS
            && indexAbstraction.getIndices().size() > 1
            && indexAbstraction.getWriteIndex() == null) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "Alias [%s] points to %d indices, and does not have a designated write index",
                    name,
                    indexAbstraction.getIndices().size()
                )
            );
        }

        Index concreteIndex = indexAbstraction.getWriteIndex();
        if (concreteIndex == null) {
            concreteIndex = indexAbstraction.getIndices().get(indexAbstraction.getIndices().size() - 1);
        }
        return metadata.getProject().index(concreteIndex);
    }

}
