/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.cluster;

import org.elasticsearch.test.cluster.local.LocalClusterSpec;

import java.util.HashMap;
import java.util.Map;

public class MutableSystemPropertyProvider implements SystemPropertyProvider {
    private final Map<String, String> settings = new HashMap<>();

    @Override
    public Map<String, String> get(LocalClusterSpec.LocalNodeSpec nodeSpec) {
        return settings;
    }

    public void put(String setting, String value) {
        settings.put(setting, value);
    }

    public void remove(String setting) {
        settings.remove(setting);
    }

    public void clear() {
        settings.clear();
    }
}
