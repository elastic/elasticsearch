/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;

import java.io.IOException;

public class MachineLearningFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public MachineLearningFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState) {
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackPlugin.MACHINE_LEARNING;
    }

    @Override
    public String description() {
        return "Machine Learning for the Elastic Stack";
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isMachineLearningAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    @Override
    public XPackFeatureSet.Usage usage() {
        return new Usage(available(), enabled());
    }

    public static class Usage extends XPackFeatureSet.Usage {

        public Usage(StreamInput input) throws IOException {
            super(input);
        }

        public Usage(boolean available, boolean enabled) {
            super(XPackPlugin.MACHINE_LEARNING, available, enabled);
        }
    }
}
