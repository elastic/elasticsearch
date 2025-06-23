/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.xpack.ml.autoscaling.AbstractNodeAvailabilityZoneMapper;
import org.elasticsearch.xpack.ml.autoscaling.NodeRealAvailabilityZoneMapper;

public class DefaultMachineLearningExtension implements MachineLearningExtension {

    public static final String[] ANALYTICS_DEST_INDEX_ALLOWED_SETTINGS = {
        IndexMetadata.SETTING_NUMBER_OF_SHARDS,
        IndexMetadata.SETTING_NUMBER_OF_REPLICAS,
        MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(),
        MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING.getKey(),
        MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING.getKey(),
        MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING.getKey(),
        MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING.getKey(),
        MapperService.INDEX_MAPPING_DIMENSION_FIELDS_LIMIT_SETTING.getKey() };

    @Override
    public boolean useIlm() {
        return true;
    }

    @Override
    public boolean includeNodeInfo() {
        return true;
    }

    @Override
    public boolean isAnomalyDetectionEnabled() {
        return true;
    }

    @Override
    public boolean isDataFrameAnalyticsEnabled() {
        return true;
    }

    @Override
    public boolean isNlpEnabled() {
        return true;
    }

    @Override
    public String[] getAnalyticsDestIndexAllowedSettings() {
        return ANALYTICS_DEST_INDEX_ALLOWED_SETTINGS;
    }

    @Override
    public AbstractNodeAvailabilityZoneMapper getNodeAvailabilityZoneMapper(Settings settings, ClusterSettings clusterSettings) {
        return new NodeRealAvailabilityZoneMapper(settings, clusterSettings);
    }
}
