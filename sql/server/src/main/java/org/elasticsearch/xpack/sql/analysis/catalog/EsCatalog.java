/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public class EsCatalog implements Catalog {

    private final Supplier<ClusterState> clusterState;
    private IndexNameExpressionResolver indexNameExpressionResolver;

    public EsCatalog(Supplier<ClusterState> clusterState) {
        this.clusterState = clusterState;
    }

    @Inject  // NOCOMMIT more to ctor and move resolver to createComponents
    public void setIndexNameExpressionResolver(IndexNameExpressionResolver resolver) {
        this.indexNameExpressionResolver = resolver;
    }

    private MetaData metadata() {
        return clusterState.get().getMetaData();
    }

    @Override
    public EsIndex getIndex(String index) throws SqlIllegalArgumentException {
        MetaData metadata = metadata();
        IndexMetaData idx = metadata.index(index);
        if (idx == null) {
            return null;
        }
        return EsIndex.build(idx, singleType(idx, false));
    }

    @Override
    public List<EsIndex> listIndices(String pattern) {
        Iterator<IndexMetaData> indexMetadata = null;
        MetaData md = metadata();
        if (pattern == null) {
            indexMetadata = md.indices().valuesIt();
        }
        else {
            String[] indexNames = resolveIndex(pattern);
            List<IndexMetaData> indices = new ArrayList<>(indexNames.length);
            for (String indexName : indexNames) {
                 indices.add(md.index(indexName));    
            }
            indexMetadata = indices.iterator();
        }

        List<EsIndex> list = new ArrayList<>();
        // filter unsupported (indices with more than one type) indices
        while (indexMetadata.hasNext()) {
            IndexMetaData imd = indexMetadata.next();
            MappingMetaData type = singleType(imd, true);
            if (type != null) {
                list.add(EsIndex.build(imd, type));
            }
        }

        return list;
    }

    /**
     * Return the single type in the index of {@code null} if there
     * are no types in the index.
     * @param badIndicesAreNull if true then return null for indices with
     *      more than one type, if false throw an exception for such indices
     */
    @Nullable
    private MappingMetaData singleType(IndexMetaData index, boolean badIndicesAreNull) {
        /* We actually ignore the _default_ mapping because it is still
         * allowed but deprecated. */
        MappingMetaData result = null;
        List<String> typeNames = null;
        for (ObjectObjectCursor<String, MappingMetaData> type : index.getMappings()) {
            if ("_default_".equals(type.key)) {
                continue;
            }
            if (result != null) {
                if (badIndicesAreNull) {
                    return null;
                }
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
        throw new IllegalArgumentException("[" + index.getIndex().getName() + "] has more than one type " + typeNames);
    }

    private String[] resolveIndex(String pattern) {
        // NOCOMMIT we should use the cluster state that we resolve when we fetch the metadata so it is the *same* so we don't have weird errors when indices are deleted
        return indexNameExpressionResolver.concreteIndexNames(clusterState.get(), IndicesOptions.strictExpandOpenAndForbidClosed(), pattern);
    }
}