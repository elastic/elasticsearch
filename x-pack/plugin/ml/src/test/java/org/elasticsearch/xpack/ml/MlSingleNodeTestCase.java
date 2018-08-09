/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.license.LicenseService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ml.MachineLearningField;

import java.util.Collection;

/**
 * An extention to {@link ESSingleNodeTestCase} that adds node settings specifically needed for ML test cases.
 */
public abstract class MlSingleNodeTestCase extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        Settings.Builder newSettings = Settings.builder();
        newSettings.put(super.nodeSettings());

        // Disable native ML autodetect_process as the c++ controller won't be available
        newSettings.put(MachineLearningField.AUTODETECT_PROCESS.getKey(), false);
        newSettings.put(MachineLearningField.MAX_MODEL_MEMORY_LIMIT.getKey(), new ByteSizeValue(1024));
        newSettings.put(LicenseService.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial");
        // Disable security otherwise delete-by-query action fails to get authorized
        newSettings.put(XPackSettings.SECURITY_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.MONITORING_ENABLED.getKey(), false);
        newSettings.put(XPackSettings.WATCHER_ENABLED.getKey(), false);
        return newSettings.build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(LocalStateMachineLearning.class);
    }

    protected void waitForMlTemplates() throws Exception {
        // block until the templates are installed
        assertBusy(() -> {
            ClusterState state = client().admin().cluster().prepareState().get().getState();
            assertTrue("Timed out waiting for the ML templates to be installed",
                    MachineLearning.allTemplatesInstalled(state));
        });
    }

}
