/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.cluster;

import org.elasticsearch.test.cluster.local.LocalClusterSpec;

import java.util.HashMap;
import java.util.Map;

public class MutableSettingsProvider implements SettingsProvider {
    private final Map<String, String> settings = new HashMap<>();

    @Override
    public Map<String, String> get(LocalClusterSpec.LocalNodeSpec nodeSpec) {
        return settings;
    }

    public void put(String setting, String value) {
        settings.put(setting, value);
    }

    public void clear() {
        settings.clear();
    }
}
