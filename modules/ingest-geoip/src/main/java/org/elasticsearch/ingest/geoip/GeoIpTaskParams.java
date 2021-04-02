/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.persistent.PersistentTaskParams;

import java.io.IOException;

import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;

class GeoIpTaskParams implements PersistentTaskParams {

    public static final ObjectParser<GeoIpTaskParams, Void> PARSER = new ObjectParser<>(GEOIP_DOWNLOADER, true, GeoIpTaskParams::new);

    GeoIpTaskParams() {
    }

    GeoIpTaskParams(StreamInput in) {
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return GEOIP_DOWNLOADER;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_7_13_0;
    }

    @Override
    public void writeTo(StreamOutput out) {
    }

    public static GeoIpTaskParams fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof GeoIpTaskParams;
    }
}
