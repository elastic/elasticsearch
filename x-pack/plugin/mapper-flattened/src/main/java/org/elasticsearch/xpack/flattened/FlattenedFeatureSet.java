/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.flattened;

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
import org.elasticsearch.xpack.core.flattened.FlattenedFeatureSetUsage;
import org.elasticsearch.xpack.flattened.mapper.FlatObjectFieldMapper;

import java.util.Map;

public class FlattenedFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final ClusterService clusterService;

    @Inject
    public FlattenedFeatureSet(Settings settings, XPackLicenseState licenseState, ClusterService clusterService) {
        this.enabled = XPackSettings.FLATTENED_ENABLED.get(settings);
        this.licenseState = licenseState;
        this.clusterService = clusterService;
    }

    @Override
    public String name() {
        return XPackField.FLATTENED;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isFlattenedAllowed();
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
    public void usage(ActionListener<Usage> listener) {
        int fieldCount = 0;
        if (available() && enabled() && clusterService.state() != null) {
            for (IndexMetaData indexMetaData : clusterService.state().metaData()) {
                MappingMetaData mappingMetaData = indexMetaData.mapping();

                if (mappingMetaData != null) {
                    Map<String, Object> mappings = mappingMetaData.getSourceAsMap();

                    if (mappings.containsKey("properties")) {
                        @SuppressWarnings("unchecked")
                        Map<String, Map<String, Object>> fieldMappings = (Map<String, Map<String, Object>>) mappings.get("properties");

                        for (Map<String, Object> fieldMapping : fieldMappings.values()) {
                            String fieldType = (String) fieldMapping.get("type");
                            if (fieldType != null && fieldType.equals(FlatObjectFieldMapper.CONTENT_TYPE)) {
                                fieldCount++;
                            }
                        }
                    }
                }
            }
        }

        listener.onResponse(new FlattenedFeatureSetUsage(available(), enabled(), fieldCount));
    }
}
