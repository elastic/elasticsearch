/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reservedstate.service;

import org.elasticsearch.features.FeatureSpecification;
import org.elasticsearch.features.NodeFeature;

import java.util.Set;

public class FileSettingsFeatures implements FeatureSpecification {

    // Although file settings were supported starting in 8.4.0, this is really about whether file settings
    // are used in readiness.
    public static final NodeFeature FILE_SETTINGS_SUPPORTED = new NodeFeature("file_settings");

    @Override
    public Set<NodeFeature> getFeatures() {
        return Set.of(FILE_SETTINGS_SUPPORTED);
    }
}
