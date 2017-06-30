/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.analysis.catalog;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Strings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public class EsCatalog implements Catalog {

    private static final String WILDCARD = "*";

    private final Supplier<ClusterState> clusterState;
    private IndexNameExpressionResolver indexNameExpressionResolver;

    public EsCatalog(Supplier<ClusterState> clusterState) {
        this.clusterState = clusterState;
    }

    // initialization hack
    public void setIndexNameExpressionResolver(IndexNameExpressionResolver resolver) {
        this.indexNameExpressionResolver = resolver;
    }

    private MetaData metadata() {
        return clusterState.get().getMetaData();
    }

    @Override
    public EsIndex getIndex(String index) {
        if (!indexExists(index)) {
            return EsIndex.NOT_FOUND;
        }
        return EsIndex.build(metadata().index(index));
    }

    @Override
    public boolean indexExists(String index) {
        MetaData meta = metadata();
        return meta.hasIndex(index);
    }

    @Override
    public Collection<EsIndex> listIndices() {
        return listIndices(null);
    }

    @Override
    public Collection<EsIndex> listIndices(String pattern) {
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

        return EsIndex.build(indexMetadata);
    }

    @Override
    public EsType getType(String index, String type) {
        if (!indexExists(index)) {
            return EsType.NOT_FOUND;
        }
        
        // NOCOMMIT verify that this works if the index isn't on the node
        MappingMetaData mapping = metadata().index(index).mapping(type);
        if (mapping == null) {
            return EsType.NOT_FOUND;
        }
        return EsType.build(index, type, mapping);
    }

    @Override
    public boolean typeExists(String index, String type) {
        return indexExists(index) && metadata().index(index).getMappings().containsKey(type);
    }

    @Override
    public Collection<EsType> listTypes(String index) {
        return listTypes(index, null);
    }

    @Override
    public Collection<EsType> listTypes(String indexPattern, String pattern) {
        if (!Strings.hasText(indexPattern)) {
            indexPattern = WILDCARD;
        }

        String[] iName = indexNameExpressionResolver.concreteIndexNames(clusterState.get(), IndicesOptions.strictExpandOpenAndForbidClosed(), indexPattern);
        
        List<EsType> types = new ArrayList<>();

        for (String cIndex : iName) {
            IndexMetaData imd = metadata().index(cIndex);

            if (Strings.hasText(pattern)) {
                types.add(EsType.build(cIndex, pattern, imd.mapping(pattern)));
            }
            else {
                types.addAll(EsType.build(cIndex, imd.getMappings()));
            }
        }

        return types;
    }
    
    private String[] resolveIndex(String pattern) {
        return indexNameExpressionResolver.concreteIndexNames(clusterState.get(), IndicesOptions.strictExpandOpenAndForbidClosed(), pattern);
    }
}