/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.cluster.stateless.local;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.elasticsearch.test.cluster.local.DefaultSettingsProvider;
import org.elasticsearch.test.cluster.local.LocalClusterSpec;

import java.util.List;
import java.util.Map;
import java.util.Random;

public class DefaultStatelessSettingsProvider extends DefaultSettingsProvider {
    private static final List<String> EXCLUDED_SETTINGS = List.of("cluster.deprecation_indexing.enabled");

    @Override
    public Map<String, String> get(LocalClusterSpec.LocalNodeSpec nodeSpec) {
        Map<String, String> defaultSettings = super.get(nodeSpec);
        initUploadMaxCommitsSettings(defaultSettings);
        EXCLUDED_SETTINGS.forEach(defaultSettings::remove);
        return defaultSettings;
    }

    private boolean initialized;
    private int uploadMaxCommits;

    private synchronized void initUploadMaxCommitsSettings(Map<String, String> settings) {
        if (initialized == false) {
            final Random random = RandomizedContext.current().getRandom();
            initialized = true;
            uploadMaxCommits = random.nextBoolean() ? random.nextInt(1, 10) : 100;
        }
        settings.put("stateless.upload.max_commits", String.valueOf(uploadMaxCommits));
    }
}
