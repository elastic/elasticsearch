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
import java.util.Map;
import java.util.Objects;

public class EnterpriseSearchFeatureSetUsage extends XPackFeatureSet.Usage {

    public static final String SEARCH_APPLICATIONS = "search_applications";
    private final Map<String, Object> searchApplicationsStats;

    public EnterpriseSearchFeatureSetUsage(boolean available, boolean enabled, Map<String, Object> searchApplicationsStats) {
        super(XPackField.ENTERPRISE_SEARCH, available, enabled);
        this.searchApplicationsStats = Objects.requireNonNull(searchApplicationsStats);
    }

    public EnterpriseSearchFeatureSetUsage(StreamInput in) throws IOException {
        super(in);
        this.searchApplicationsStats = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeGenericMap(searchApplicationsStats);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.V_8_8_0;
    }

    @Override
    protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
        super.innerXContent(builder, params);
        builder.field(SEARCH_APPLICATIONS, searchApplicationsStats);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnterpriseSearchFeatureSetUsage that = (EnterpriseSearchFeatureSetUsage) o;
        return Objects.equals(searchApplicationsStats, that.searchApplicationsStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(searchApplicationsStats);
    }

    public Map<String, Object> getSearchApplicationsStats() {
        return searchApplicationsStats;
    }
}
