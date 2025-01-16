/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

/**
 * As a relatively minor hack, this class holds the string constant that defines both the id
 * and the name of the task for the new ip geolocation database downloader feature. It also provides the
 * PersistentTaskParams that are necessary to start the task and to run it.
 * <p>
 * Defining this in Elasticsearch itself gives us a reasonably tidy version of things where we don't
 * end up with strange inter-module dependencies. It's not ideal, but it works fine.
 */
public final class EnterpriseGeoIpTask {

    private EnterpriseGeoIpTask() {
        // utility class
    }

    public static final String ENTERPRISE_GEOIP_DOWNLOADER = "enterprise-geoip-downloader";

    public static class EnterpriseGeoIpTaskParams implements PersistentTaskParams {

        public static final ObjectParser<EnterpriseGeoIpTaskParams, Void> PARSER = new ObjectParser<>(
            ENTERPRISE_GEOIP_DOWNLOADER,
            true,
            EnterpriseGeoIpTaskParams::new
        );

        public EnterpriseGeoIpTaskParams() {}

        public EnterpriseGeoIpTaskParams(StreamInput in) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return ENTERPRISE_GEOIP_DOWNLOADER;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersions.V_8_16_0;
        }

        @Override
        public void writeTo(StreamOutput out) {}

        public static EnterpriseGeoIpTaskParams fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof EnterpriseGeoIpTaskParams;
        }
    }
}
