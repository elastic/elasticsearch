/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

/**
 * Provides the XPack features that this version of the code supports
 */
public class XPackFeatures implements FeatureSpecification {

    public static final NodeFeature AGGREGATE_METRIC_DOUBLE_DEPRECATED_DEFAULT_METRIC = new NodeFeature(
        "aggregate_metric_double.default_metric.deprecated"
    );

    public static final NodeFeature VECTORDB_DOCUMENT_USAGE = new NodeFeature("vectordb_document.usage");

    public static final NodeFeature LOGSDB_COLUMNAR_USAGE = new NodeFeature("logsdb_columnar.usage");

    public static final NodeFeature COLUMNAR_ENABLED_SETTING = new NodeFeature("columnar.enabled_setting");

    public static final NodeFeature COLUMNAR_USAGE = new NodeFeature("columnar.usage");

    public static final NodeFeature VECTORDB_COLUMNAR_USAGE = new NodeFeature("vectordb_columnar.usage");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(AGGREGATE_METRIC_DOUBLE_DEPRECATED_DEFAULT_METRIC);
    }

    @Override
    public Set<NodeFeature> getTestFeatures() {
        // VECTORDB_COLUMNAR_USAGE is advertised regardless of the vectordb_columnar feature flag: the yaml test runner rejects
        // features that are missing from the build's feature metadata, so a flag-gated test feature fails rather than skips in
        // release builds. Tests that need the index mode itself gate on the vectordb_columnar_index_mode capability instead.
        return Set.of(VECTORDB_DOCUMENT_USAGE, COLUMNAR_ENABLED_SETTING, LOGSDB_COLUMNAR_USAGE, COLUMNAR_USAGE, VECTORDB_COLUMNAR_USAGE);
    }
}
