/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexMode;

import java.util.HashSet;
import java.util.Set;

/**
 * Provides the XPack features that this version of the code supports
 */
public class XPackFeatures implements FeatureSpecification {

    public static final NodeFeature AGGREGATE_METRIC_DOUBLE_DEPRECATED_DEFAULT_METRIC = new NodeFeature(
        "aggregate_metric_double.default_metric.deprecated"
    );

    public static final NodeFeature VECTORDB_DOCUMENT_USAGE = new NodeFeature("vectordb_document.usage");

    public static final NodeFeature COLUMNAR_ENABLED_SETTING = new NodeFeature("columnar.enabled_setting");

    public static final NodeFeature VECTORDB_COLUMNAR_USAGE = new NodeFeature("vectordb_columnar.usage");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(AGGREGATE_METRIC_DOUBLE_DEPRECATED_DEFAULT_METRIC);
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        Set<NodeFeature> features = new HashSet<>(Set.of(VECTORDB_DOCUMENT_USAGE, COLUMNAR_ENABLED_SETTING));
        // Advertised only when the mode can actually be used, so a yaml test gating on this feature also skips on builds
        // where vectordb_columnar indices cannot be created.
        if (IndexMode.VECTORDB_COLUMNAR_FEATURE_FLAG.isEnabled()) {
            features.add(VECTORDB_COLUMNAR_USAGE);
        }
        return Set.copyOf(features);
    }
}
