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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;

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
    public Collection<EsType> listTypes(String indexPattern, String typePattern) {
        if (!Strings.hasText(indexPattern)) {
            indexPattern = WILDCARD;
        }

        String[] indices = indexNameExpressionResolver.concreteIndexNames(clusterState.get(),
                IndicesOptions.strictExpandOpenAndForbidClosed(), indexPattern);

        List<EsType> results = new ArrayList<>();
        for (String index : indices) {
            IndexMetaData imd = metadata().index(index);
            for (ObjectObjectCursor<String, MappingMetaData> entry : imd.getMappings()) {
                if (false == Strings.hasLength(typePattern) || Regex.simpleMatch(typePattern, entry.key)) {
                    results.add(EsType.build(index, entry.key, entry.value));
                }
            }
        }
        return results;
    }
    
    private String[] resolveIndex(String pattern) {
        return indexNameExpressionResolver.concreteIndexNames(clusterState.get(), IndicesOptions.strictExpandOpenAndForbidClosed(),
                pattern);
    }
}