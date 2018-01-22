/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.logstash;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.XPackSettings;


import java.io.IOException;
import java.util.Map;

public class LogstashFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public LogstashFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState) {
        this.enabled = XPackSettings.LOGSTASH_ENABLED.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return Logstash.NAME;
    }

    @Override
    public String description() {
        return "Logstash management component for X-Pack";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isLogstashAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
      return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        listener.onResponse(new LogstashFeatureSet.Usage(available(), enabled()));
    }

    public static class Usage extends XPackFeatureSet.Usage {

        public Usage(StreamInput in) throws IOException {
            super(in);
        }

        public Usage(boolean available, boolean enabled) {
            super(Logstash.NAME, available, enabled);
        }

        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        protected void innerXContent(XContentBuilder builder, Params params) throws IOException {
            super.innerXContent(builder, params);
        }
    }
}
