/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.core.Predicates.never;

/**
 * Holder for the data stream global settings relating to the data stream failure store. This defines, validates, and monitors the settings.
 */
public class DataStreamFailureStoreSettings {

    private static final Logger logger = LogManager.getLogger(DataStreamFailureStoreSettings.class);

    public static final Setting<List<String>> DATA_STREAM_FAILURE_STORED_ENABLED_SETTING = Setting.stringListSetting(
        "data_streams.failure_store.enabled",
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private Predicate<String> failureStoreEnabledByName;

    private DataStreamFailureStoreSettings() {
        failureStoreEnabledByName = never();
    }

    /**
     * Creates an instance and initialises the cluster settings listeners.
     *
     * @param clusterSettings The cluster settings to initialize the instance from and to watch for updates to
     */
    public static DataStreamFailureStoreSettings create(ClusterSettings clusterSettings) {
        DataStreamFailureStoreSettings dataStreamFailureStoreSettings = new DataStreamFailureStoreSettings();
        clusterSettings.initializeAndWatch(
            DATA_STREAM_FAILURE_STORED_ENABLED_SETTING,
            dataStreamFailureStoreSettings::setEnabledByNamePatterns
        );
        return dataStreamFailureStoreSettings;
    }

    /**
     * Returns whether the settings indicate that the failure store should be enabled by the cluster settings for the given name.
     *
     * @param name The data stream name
     */
    public boolean failureStoreEnabledForDataStreamName(String name) {
        return failureStoreEnabledByName.test(name);
    }

    private void setEnabledByNamePatterns(List<String> patterns) {
        failureStoreEnabledByName = Regex.simpleMatcher(patterns.toArray(String[]::new));
        logger.info("Updated data stream name patterns for enabling failure store to [{}]", patterns);
    }
}
