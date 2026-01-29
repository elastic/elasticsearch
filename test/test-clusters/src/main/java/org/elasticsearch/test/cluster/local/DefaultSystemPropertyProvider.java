/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster.local;

import org.elasticsearch.test.cluster.SystemPropertyProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DefaultSystemPropertyProvider implements SystemPropertyProvider {
    @Override
    public Map<String, String> get(LocalClusterSpec.LocalNodeSpec nodeSpec) {
        Map<String, String> properties = new HashMap<>();
        properties.put("ingest.geoip.downloader.enabled.default", "false");

        // enable test features unless we are running forwards compatibility tests
        if (Boolean.parseBoolean(System.getProperty("tests.fwc", "false")) == false) {
            properties.put("tests.testfeatures.enabled", "true");
        }

        return Collections.unmodifiableMap(properties);
    }
}
