/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;

import java.io.IOException;
import java.util.Map;

public class EnrichFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public EnrichFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState) {
        this.enabled = XPackSettings.ENRICH_ENABLED_SETTING.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.ENRICH;
    }

    @Override
    public boolean available() {
        return licenseState.isEnrichAllowed();
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
        listener.onResponse(new Usage(available(), enabled()));
    }

    public static class Usage extends XPackFeatureSet.Usage {

        Usage(boolean available, boolean enabled) {
            super(XPackField.ENRICH, available, enabled);
        }

        public Usage(StreamInput input) throws IOException {
            super(input);
        }
    }
}
