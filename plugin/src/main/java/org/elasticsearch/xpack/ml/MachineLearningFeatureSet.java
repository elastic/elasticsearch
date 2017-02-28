/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.XPackFeatureSet;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.job.process.NativeController;
import org.elasticsearch.xpack.ml.job.process.NativeControllerHolder;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class MachineLearningFeatureSet implements XPackFeatureSet {

    private final boolean enabled;
    private final XPackLicenseState licenseState;
    private final Map<String, Object> nativeCodeInfo;

    @Inject
    public MachineLearningFeatureSet(Settings settings, @Nullable XPackLicenseState licenseState) {
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(settings);
        this.licenseState = licenseState;
        Map<String, Object> nativeCodeInfo = NativeController.UNKNOWN_NATIVE_CODE_INFO;
        // Don't try to get the native code version in the transport client - the controller process won't be running
        if (XPackPlugin.transportClientMode(settings) == false && XPackPlugin.isTribeClientNode(settings) == false) {
            try {
                NativeController nativeController = NativeControllerHolder.getNativeController(settings);
                if (nativeController != null) {
                    nativeCodeInfo = nativeController.getNativeCodeInfo();
                }
            } catch (IOException | TimeoutException e) {
                Loggers.getLogger(MachineLearningFeatureSet.class).error("Cannot get native code info for Machine Learning", e);
                if (enabled) {
                    throw new ElasticsearchException("Cannot communicate with Machine Learning native code "
                            + "- please check that you are running on a supported platform");
                }
            }
        }
        this.nativeCodeInfo = nativeCodeInfo;
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
    public Map<String, Object> nativeCodeInfo() {
        return nativeCodeInfo;
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
