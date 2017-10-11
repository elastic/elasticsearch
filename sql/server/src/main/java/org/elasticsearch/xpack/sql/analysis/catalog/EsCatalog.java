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
import org.elasticsearch.xpack.sql.SqlException;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.Types;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EsCatalog implements Catalog {
    private final ClusterState clusterState;

    public EsCatalog(ClusterState clusterState) {
        this.clusterState = clusterState;
    }

    @Override
    public GetIndexResult getIndex(String index) throws SqlException {
        IndexMetaData idx = clusterState.getMetaData().index(index);
        if (idx == null) {
            return GetIndexResult.notFound(index);
        }
        if (idx.getIndex().getName().startsWith(".")) {
            /* Indices that start with "." are considered internal and
             * should not be available to SQL. */
            return GetIndexResult.invalid(
                    "[" + idx.getIndex().getName() + "] starts with [.] so it is considered internal and incompatible with sql");
        }

        // Make sure that the index contains only a single type
        MappingMetaData singleType = null;
        List<String> typeNames = null;
        for (ObjectObjectCursor<String, MappingMetaData> type : idx.getMappings()) {
            /* We actually ignore the _default_ mapping because it is still
             * allowed but deprecated. */
            if ("_default_".equals(type.key)) {
                continue;
            }
            if (singleType != null) {
                // There are more than one types
                if (typeNames == null) {
                    typeNames = new ArrayList<>();
                    typeNames.add(singleType.type());
                }
                typeNames.add(type.key);
            }
            singleType = type.value;
        }
        if (singleType == null) {
            return GetIndexResult.invalid("[" + idx.getIndex().getName() + "] doesn't have any types so it is incompatible with sql");
        }
        if (typeNames != null) {
            Collections.sort(typeNames);
            return GetIndexResult.invalid(
                    "[" + idx.getIndex().getName() + "] contains more than one type " + typeNames + " so it is incompatible with sql");
        }
        Map<String, DataType> mapping = Types.fromEs(singleType.sourceAsMap());
        List<String> aliases = Arrays.asList(idx.getAliases().keys().toArray(String.class));
        return GetIndexResult.valid(new EsIndex(idx.getIndex().getName(), mapping, aliases, idx.getSettings()));
    }
}