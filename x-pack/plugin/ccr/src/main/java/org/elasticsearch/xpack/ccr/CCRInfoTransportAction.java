/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;
import org.elasticsearch.xpack.core.ccr.CcrConstants;

import java.io.IOException;
import java.util.Objects;

public class CCRInfoTransportAction extends XPackInfoFeatureTransportAction {

    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public CCRInfoTransportAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Settings settings,
        XPackLicenseState licenseState
    ) {
        super(XPackInfoFeatureAction.CCR.name(), transportService, actionFilters);
        this.enabled = XPackSettings.CCR_ENABLED_SETTING.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.CCR;
    }

    @Override
    public boolean available() {
        return CcrConstants.CCR_FEATURE.checkWithoutTracking(licenseState);
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    public static class Usage extends XPackFeatureSet.Usage {

        private final int numberOfFollowerIndices;
        private final int numberOfAutoFollowPatterns;
        private final Long lastFollowTimeInMillis;

        public Usage(
            boolean available,
            boolean enabled,
            int numberOfFollowerIndices,
            int numberOfAutoFollowPatterns,
            Long lastFollowTimeInMillis
        ) {
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

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.V_7_0_0;
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
            return numberOfFollowerIndices == usage.numberOfFollowerIndices
                && numberOfAutoFollowPatterns == usage.numberOfAutoFollowPatterns
                && Objects.equals(lastFollowTimeInMillis, usage.lastFollowTimeInMillis);
        }

        @Override
        public int hashCode() {
            return Objects.hash(numberOfFollowerIndices, numberOfAutoFollowPatterns, lastFollowTimeInMillis);
        }
    }
}
