/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

/**
 * An extention to {@link ESSingleNodeTestCase} that adds node settings specifically needed for x-pack
 */
public abstract class XPackSingleNodeTestCase extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings()  {
        Settings.Builder newSettings = Settings.builder();
        // Disable native ML autodetect_process as the c++ controller won't be available
        newSettings.put(MachineLearning.AUTODETECT_PROCESS.getKey(), false);
        return newSettings.build();
    }
}
