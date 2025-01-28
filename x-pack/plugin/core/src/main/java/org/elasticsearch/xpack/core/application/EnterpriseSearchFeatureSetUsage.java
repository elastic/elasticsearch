/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.application;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureUsage;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class EnterpriseSearchFeatureSetUsage extends XPackFeatureUsage {

    static final TransportVersion BEHAVIORAL_ANALYTICS_TRANSPORT_VERSION = TransportVersions.V_8_8_1;
    static final TransportVersion QUERY_RULES_TRANSPORT_VERSION = TransportVersions.V_8_10_X;

    public static final String SEARCH_APPLICATIONS = "search_applications";
    public static final String ANALYTICS_COLLECTIONS = "analytics_collections";
    public static final String QUERY_RULESETS = "query_rulesets";
    public static final String COUNT = "count";
    public static final String TOTAL_COUNT = "total_count";
    public static final String TOTAL_RULE_COUNT = "total_rule_count";
    public static final String MIN_RULE_COUNT = "min_rule_count";
    public static final String MAX_RULE_COUNT = "max_rule_count";
    public static final String RULE_CRITERIA_TOTAL_COUNTS = "rule_criteria_total_counts";
    public static final String RULE_TYPE_TOTAL_COUNTS = "rule_type_total_counts";

    private final Map<String, Object> searchApplicationsUsage;
    private final Map<String, Object> analyticsCollectionsUsage;
    private final Map<String, Object> queryRulesUsage;

    public EnterpriseSearchFeatureSetUsage(
        boolean available,
        boolean enabled,
        Map<String, Object> searchApplicationsUsage,
        Map<String, Object> analyticsCollectionsUsage,
        Map<String, Object> queryRulesUsage
    ) {
        super(XPackField.ENTERPRISE_SEARCH, available, enabled);
        this.searchApplicationsUsage = Objects.requireNonNull(searchApplicationsUsage);
        this.analyticsCollectionsUsage = Objects.requireNonNull(analyticsCollectionsUsage);
        this.queryRulesUsage = Objects.requireNonNull(queryRulesUsage);
    }

    public EnterpriseSearchFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.searchApplicationsUsage = in.readGenericMap();
        Map<String, Object> analyticsCollectionsUsage = new HashMap<>();
        Map<String, Object> queryRulesUsage = new HashMap<>();
        if (in.getTransportVersion().onOrAfter(QUERY_RULES_TRANSPORT_VERSION)) {
            analyticsCollectionsUsage = in.readGenericMap();
            queryRulesUsage = in.readGenericMap();
        } else if (in.getTransportVersion().onOrAfter(BEHAVIORAL_ANALYTICS_TRANSPORT_VERSION)) {
            analyticsCollectionsUsage = in.readGenericMap();
        }
        this.analyticsCollectionsUsage = analyticsCollectionsUsage;
        this.queryRulesUsage = queryRulesUsage;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(searchApplicationsUsage);
        if (out.getTransportVersion().onOrAfter(BEHAVIORAL_ANALYTICS_TRANSPORT_VERSION)) {
            out.writeGenericMap(analyticsCollectionsUsage);
        }
        if (out.getTransportVersion().onOrAfter(QUERY_RULES_TRANSPORT_VERSION)) {
            out.writeGenericMap(queryRulesUsage);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_8_0;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field(SEARCH_APPLICATIONS, searchApplicationsUsage);
        builder.field(ANALYTICS_COLLECTIONS, analyticsCollectionsUsage);
        builder.field(QUERY_RULESETS, queryRulesUsage);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnterpriseSearchFeatureSetUsage that = (EnterpriseSearchFeatureSetUsage) o;
        return Objects.equals(searchApplicationsUsage, that.searchApplicationsUsage)
            && Objects.equals(analyticsCollectionsUsage, that.analyticsCollectionsUsage)
            && Objects.equals(queryRulesUsage, that.queryRulesUsage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchApplicationsUsage, analyticsCollectionsUsage, queryRulesUsage);
    }

    public Map<String, Object> getSearchApplicationsUsage() {
        return searchApplicationsUsage;
    }

    public Map<String, Object> getAnalyticsCollectionsUsage() {
        return analyticsCollectionsUsage;
    }

    public Map<String, Object> getQueryRulesUsage() {
        return queryRulesUsage;
    }
}
