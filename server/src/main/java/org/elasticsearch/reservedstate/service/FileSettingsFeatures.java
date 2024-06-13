/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.Version;
import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Map;

public class FileSettingsFeatures implements FeatureSpecification {

    // Although file settings were supported starting in 8.4.0, this is really about whether file settings
    // are used in readiness.
    public static final NodeFeature FILE_SETTINGS_SUPPORTED = new NodeFeature("file_settings");

    @Override
    public Map<NodeFeature, Version> getHistoricalFeatures() {
        return Map.of(FILE_SETTINGS_SUPPORTED, Version.V_8_4_0);
    }
}
