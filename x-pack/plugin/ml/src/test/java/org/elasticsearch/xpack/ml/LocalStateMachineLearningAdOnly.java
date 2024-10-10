/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.ml.MachineLearningTests.MlTestExtension;
import org.elasticsearch.xpack.ml.MachineLearningTests.MlTestExtensionLoader;

import java.nio.file.Path;

public class LocalStateMachineLearningAdOnly extends LocalStateMachineLearning {
    public LocalStateMachineLearningAdOnly(final Settings settings, final Path configPath) {
        super(settings, configPath, new MlTestExtensionLoader(new MlTestExtension(true, true, true, false, false)));
    }
}
