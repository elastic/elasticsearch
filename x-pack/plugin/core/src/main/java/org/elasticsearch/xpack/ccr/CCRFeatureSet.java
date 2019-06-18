/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

public class CCRFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public CCRFeatureSet(Settings settings, XPackLicenseState licenseState) {
        this.enabled = XPackSettings.CCR_ENABLED_SETTING.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.CCR;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isCcrAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    public static class UsageTransportAction extends XPackUsageFeatureTransportAction {

        private final Settings settings;
        private final XPackLicenseState licenseState;

        @Inject
        public UsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                    Settings settings, XPackLicenseState licenseState) {
            super(XPackUsageFeatureAction.CCR.name(), transportService, clusterService,
                threadPool, actionFilters, indexNameExpressionResolver);
            this.settings = settings;
            this.licenseState = licenseState;
        }

        @Override
        protected void masterOperation(XPackUsageRequest request, ClusterState state, ActionListener<XPackUsageFeatureResponse> listener) {
            MetaData metaData = state.metaData();

            int numberOfFollowerIndices = 0;
            long lastFollowerIndexCreationDate = 0L;
            for (IndexMetaData imd : metaData) {
                if (imd.getCustomData("ccr") != null) {
                    numberOfFollowerIndices++;
                    if (lastFollowerIndexCreationDate < imd.getCreationDate()) {
                        lastFollowerIndexCreationDate = imd.getCreationDate();
                    }
                }
            }
            AutoFollowMetadata autoFollowMetadata = metaData.custom(AutoFollowMetadata.TYPE);
            int numberOfAutoFollowPatterns = autoFollowMetadata != null ? autoFollowMetadata.getPatterns().size() : 0;

            Long lastFollowTimeInMillis;
            if (numberOfFollowerIndices == 0) {
                // Otherwise we would return a value that makes no sense.
                lastFollowTimeInMillis = null;
            } else {
                lastFollowTimeInMillis = Math.max(0, Instant.now().toEpochMilli() - lastFollowerIndexCreationDate);
            }

            Usage usage = new Usage(licenseState.isCcrAllowed(), XPackSettings.CCR_ENABLED_SETTING.get(settings),
                numberOfFollowerIndices, numberOfAutoFollowPatterns, lastFollowTimeInMillis);
            listener.onResponse(new XPackUsageFeatureResponse(usage));
        }
    }

    public static class Usage extends XPackFeatureSet.Usage {

        private final int numberOfFollowerIndices;
        private final int numberOfAutoFollowPatterns;
        private final Long lastFollowTimeInMillis;

        public Usage(boolean available,
                     boolean enabled,
                     int numberOfFollowerIndices,
                     int numberOfAutoFollowPatterns,
                     Long lastFollowTimeInMillis) {
            super(XPackField.CCR, available, enabled);
            this.numberOfFollowerIndices = numberOfFollowerIndices;
            this.numberOfAutoFollowPatterns = numberOfAutoFollowPatterns;
            this.lastFollowTimeInMillis = lastFollowTimeInMillis;
        }

        public Usage(StreamInput in) throws IOException {
            super(in);
            numberOfFollowerIndices = in.readVInt();
            numberOfAutoFollowPatterns = in.readVInt();
            if (in.readBoolean()) {
                lastFollowTimeInMillis = in.readVLong();
            } else {
                lastFollowTimeInMillis = null;
            }
        }

        public int getNumberOfFollowerIndices() {
            return numberOfFollowerIndices;
        }

        public int getNumberOfAutoFollowPatterns() {
            return numberOfAutoFollowPatterns;
        }

        public Long getLastFollowTimeInMillis() {
            return lastFollowTimeInMillis;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(numberOfFollowerIndices);
            out.writeVInt(numberOfAutoFollowPatterns);
            if (lastFollowTimeInMillis != null) {
                out.writeBoolean(true);
                out.writeVLong(lastFollowTimeInMillis);
            } else {
                out.writeBoolean(false);
            }
        }

        @Override
        protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
            super.innerXContent(builder, params);
            builder.field("follower_indices_count", numberOfFollowerIndices);
            builder.field("auto_follow_patterns_count", numberOfAutoFollowPatterns);
            if (lastFollowTimeInMillis != null) {
                builder.field("last_follow_time_in_millis", lastFollowTimeInMillis);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Usage usage = (Usage) o;
            return numberOfFollowerIndices == usage.numberOfFollowerIndices &&
                numberOfAutoFollowPatterns == usage.numberOfAutoFollowPatterns &&
                Objects.equals(lastFollowTimeInMillis, usage.lastFollowTimeInMillis);
        }

        @Override
        public int hashCode() {
            return Objects.hash(numberOfFollowerIndices, numberOfAutoFollowPatterns, lastFollowTimeInMillis);
        }
    }
}
