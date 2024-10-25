/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.util.LazyInitializable;

import java.util.HashMap;
import java.util.Map;

public class DefaultSettingsConfiguration {
    public static Map<String, SettingsConfiguration> get() throws Exception {
        return configuration.getOrCompute();
    }

    private static final LazyInitializable<Map<String, SettingsConfiguration>, ?> configuration = new LazyInitializable<>(HashMap::new);
}
