/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.Platforms;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureTransportAction;

import java.util.Arrays;
import java.util.List;

public class MachineLearningInfoTransportAction extends XPackInfoFeatureTransportAction {

    /**
     * List of platforms for which the native processes are available
     */
    private static final List<String> mlPlatforms =
        Arrays.asList("darwin-x86_64", "linux-x86_64", "windows-x86_64");

    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public MachineLearningInfoTransportAction(TransportService transportService, ActionFilters actionFilters,
                                              Settings settings, XPackLicenseState licenseState) {
        super(XPackInfoFeatureAction.MACHINE_LEARNING.name(), transportService, actionFilters);
        this.enabled = XPackSettings.MACHINE_LEARNING_ENABLED.get(settings);
        this.licenseState = licenseState;
    }

    // TODO: remove these methods
    static boolean isRunningOnMlPlatform(boolean fatalIfNot) {
        return isRunningOnMlPlatform(Constants.OS_NAME, Constants.OS_ARCH, fatalIfNot);
    }

    static boolean isRunningOnMlPlatform(String osName, String osArch, boolean fatalIfNot) {
        String platformName = Platforms.platformName(osName, osArch);
        if (mlPlatforms.contains(platformName)) {
            return true;
        }
        if (fatalIfNot) {
            throw new ElasticsearchException("X-Pack is not supported and Machine Learning is not available for [" + platformName
                + "]; you can use the other X-Pack features (unsupported) by setting xpack.ml.enabled: false in elasticsearch.yml");
        }
        return false;
    }

    @Override
    public String name() {
        return XPackField.MACHINE_LEARNING;
    }

    @Override
    public boolean available() {
        return licenseState.isMachineLearningAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

}
