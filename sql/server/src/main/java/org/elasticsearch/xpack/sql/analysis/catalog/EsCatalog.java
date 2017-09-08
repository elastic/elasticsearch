/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class EsCatalog implements Catalog {
    private final ClusterState clusterState;

    public EsCatalog(ClusterState clusterState) {
        this.clusterState = clusterState;
    }

    @Override
    public EsIndex getIndex(String index) throws SqlIllegalArgumentException {
        IndexMetaData idx = clusterState.getMetaData().index(index);
        if (idx == null) {
            return null;
        }
        if (idx.getIndex().getName().startsWith(".")) {
            /* Indices that start with "." are considered internal and
             * should not be available to SQL. */
            return null;
        }
        MappingMetaData type = singleType(idx.getMappings(), idx.getIndex().getName());
        if (type == null) {
            return null;
        }
        return EsIndex.build(idx, type);
    }

    /**
     * Return the single type in the index, {@code null} if there
     * are no types in the index, and throw a {@link SqlIllegalArgumentException}
     * if there are multiple types in the index.
     */
    @Nullable
    public MappingMetaData singleType(ImmutableOpenMap<String, MappingMetaData> mappings, String name) {
        /* We actually ignore the _default_ mapping because it is still
         * allowed but deprecated. */
        MappingMetaData result = null;
        List<String> typeNames = null;
        for (ObjectObjectCursor<String, MappingMetaData> type : mappings) {
            if ("_default_".equals(type.key)) {
                continue;
            }
            if (result != null) {
                if (typeNames == null) {
                    typeNames = new ArrayList<>();
                    typeNames.add(result.type());
                }
                typeNames.add(type.key);
            }
            result = type.value;
        }
        if (typeNames == null) {
            return result;
        }
        Collections.sort(typeNames);
        throw new SqlIllegalArgumentException("[" + name + "] has more than one type " + typeNames);
    }
}