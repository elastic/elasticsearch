/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.application;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class EnterpriseSearchFeatureSetUsage extends XPackFeatureSet.Usage {

    public static final String SEARCH_APPLICATIONS = "search_applications";
    public static final String ANALYTICS_COLLECTIONS = "analytics_collections";
    public static final String COUNT = "count";
    private final Map<String, Object> searchApplicationsUsage;
    private final Map<String, Object> analyticsCollectionsUsage;

    public EnterpriseSearchFeatureSetUsage(
        boolean available,
        boolean enabled,
        Map<String, Object> searchApplicationsUsage,
        Map<String, Object> analyticsCollectionsUsage
    ) {
        super(XPackField.ENTERPRISE_SEARCH, available, enabled);
        this.searchApplicationsUsage = Objects.requireNonNull(searchApplicationsUsage);
        this.analyticsCollectionsUsage = Objects.requireNonNull(analyticsCollectionsUsage);
    }

    public EnterpriseSearchFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.searchApplicationsUsage = in.readMap();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_8_1)) {
            this.analyticsCollectionsUsage = in.readMap();
        } else {
            this.analyticsCollectionsUsage = Collections.emptyMap();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(searchApplicationsUsage);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_8_1)) {
            out.writeGenericMap(analyticsCollectionsUsage);
        }
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field(SEARCH_APPLICATIONS, searchApplicationsUsage);
        builder.field(ANALYTICS_COLLECTIONS, analyticsCollectionsUsage);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnterpriseSearchFeatureSetUsage that = (EnterpriseSearchFeatureSetUsage) o;
        return Objects.equals(searchApplicationsUsage, that.searchApplicationsUsage)
            && Objects.equals(analyticsCollectionsUsage, that.analyticsCollectionsUsage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchApplicationsUsage, analyticsCollectionsUsage);
    }

    public Map<String, Object> getSearchApplicationsUsage() {
        return searchApplicationsUsage;
    }

    public Map<String, Object> getAnalyticsCollectionsUsage() {
        return analyticsCollectionsUsage;
    }
}
