/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features.plugin;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.Plugin;

public class TestFeaturesPlugin extends Plugin {

    public static final NodeFeature TEST_FEATURES_ENABLED = new NodeFeature("features.test_features_enabled");
}
