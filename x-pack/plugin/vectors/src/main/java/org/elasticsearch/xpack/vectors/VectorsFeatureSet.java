/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.vectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.vectors.VectorsFeatureSetUsage;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper;
import org.elasticsearch.xpack.vectors.mapper.SparseVectorFieldMapper;

import java.util.Map;

public class VectorsFeatureSet implements XPackFeatureSet {

    private final ClusterService clusterService;

    @Inject
    public VectorsFeatureSet(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.VECTORS;
    }

    @Override
    public boolean available() {
        return true;
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        int numDenseVectorFields = 0;
        int numSparseVectorFields = 0;
        int avgDenseVectorDims = 0;

        if (clusterService.state() != null) {
            for (IndexMetadata indexMetadata : clusterService.state().metadata()) {
                MappingMetadata mappingMetadata = indexMetadata.mapping();
                if (mappingMetadata != null) {
                    Map<String, Object> mappings = mappingMetadata.getSourceAsMap();
                    if (mappings.containsKey("properties")) {
                        @SuppressWarnings("unchecked") Map<String, Map<String, Object>> fieldMappings =
                            (Map<String, Map<String, Object>>) mappings.get("properties");
                        for (Map<String, Object> typeDefinition : fieldMappings.values()) {
                            String fieldType = (String) typeDefinition.get("type");
                            if (fieldType != null) {
                                if (fieldType.equals(DenseVectorFieldMapper.CONTENT_TYPE)) {
                                    numDenseVectorFields++;
                                    int dims = (Integer) typeDefinition.get("dims");
                                    avgDenseVectorDims += dims;
                                } else if (fieldType.equals(SparseVectorFieldMapper.CONTENT_TYPE)) {
                                    numSparseVectorFields++;
                                }
                            }
                        }
                    }
                }
            }
            if (numDenseVectorFields > 0) {
                avgDenseVectorDims = avgDenseVectorDims / numDenseVectorFields;
            }
        }
        listener.onResponse(new VectorsFeatureSetUsage(numDenseVectorFields, numSparseVectorFields, avgDenseVectorDims));
    }
}
