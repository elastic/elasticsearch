/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.inputlatency;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

public class InputLatencyPlugin extends Plugin {

    private static final Logger log = LogManager.getLogger(InputLatencyPlugin.class);

    /** Setting for enabling or disabling simulated object store latencies. Defaults to false. */
    public static final Setting<Boolean> INPUT_LATENCY_ENABLED = Setting.boolSetting(
        "input.latency.enabled",
        false,
        Setting.Property.NodeScope
    );

    private final boolean enabled;

    public InputLatencyPlugin(Settings settings) {
        this.enabled = INPUT_LATENCY_ENABLED.get(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(INPUT_LATENCY_ENABLED);
    }

    @Override
    public void onIndexModule(IndexModule indexModule) {
        if (enabled) {
            log.debug("Enabling input latency simulation plugin");
            indexModule.setDirectoryWrapper(new SimulatedLatencyDirectoryWrapper(filename -> {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    throw new RuntimeException("Thread interrupted");
                }
            }));
        }
    }
}
