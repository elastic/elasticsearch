/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.vectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.vectors.VectorsFeatureSetUsage;
import org.elasticsearch.xpack.vectors.mapper.DenseVectorFieldMapper;
import org.elasticsearch.xpack.vectors.mapper.SparseVectorFieldMapper;

import java.util.Map;

public class VectorsFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final ClusterService clusterService;

    @Inject
    public VectorsFeatureSet(Settings settings, XPackLicenseState licenseState, ClusterService clusterService) {
        this.enabled = XPackSettings.VECTORS_ENABLED.get(settings);
        this.licenseState = licenseState;
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.VECTORS;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isVectorsAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        boolean vectorsAvailable = available();
        boolean vectorsEnabled = enabled();
        int numDenseVectorFields = 0;
        int numSparseVectorFields = 0;
        int avgDenseVectorDims = 0;

        if (vectorsAvailable && vectorsEnabled && clusterService.state() != null) {
            for (IndexMetaData indexMetaData : clusterService.state().metaData()) {
                MappingMetaData mappingMetaData = indexMetaData.mapping();
                if (mappingMetaData != null) {
                    Map<String, Object> mappings = mappingMetaData.getSourceAsMap();
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
        listener.onResponse(new VectorsFeatureSetUsage(vectorsAvailable, vectorsEnabled,
            numDenseVectorFields, numSparseVectorFields, avgDenseVectorDims));
    }
}
