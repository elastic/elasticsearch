/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.xpack.application.analytics.AnalyticsTemplateRegistry;
import org.elasticsearch.xpack.application.connector.ConnectorTemplateRegistry;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.application.rules.action.TestQueryRulesetAction.QUERY_RULES_TEST_API;

public class EnterpriseSearchFeatures implements FeatureSpecification {

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(QUERY_RULES_TEST_API);
    }

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of(
            ConnectorTemplateRegistry.CONNECTOR_TEMPLATES_FEATURE,
            Version.V_8_10_0,
            AnalyticsTemplateRegistry.ANALYTICS_TEMPLATE_FEATURE,
            Version.V_8_12_0
        );
    }
}
