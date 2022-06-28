/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.logstash;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.logstash.LogstashFeatureSetUsage;

import java.util.Map;

public class LogstashFeatureSet implements XPackFeatureSet {

    private final XPackLicenseState licenseState;

    @Inject
    public LogstashFeatureSet(@Nullable XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.LOGSTASH;
    }

    @Override
    public boolean available() {
        return Logstash.LOGSTASH_FEATURE.checkWithoutTracking(licenseState);
    }

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public Map<String, Object> nativeCodeInfo() {
        return null;
    }

    @Override
    public void usage(ActionListener<XPackFeatureSet.Usage> listener) {
        listener.onResponse(new LogstashFeatureSetUsage(available()));
    }

}
