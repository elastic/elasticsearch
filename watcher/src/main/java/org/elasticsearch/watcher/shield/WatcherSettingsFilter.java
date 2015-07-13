/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.shield;

import org.elasticsearch.common.inject.Inject;

/**
 *
 */
public interface WatcherSettingsFilter {

    void filterOut(String... patterns);

    class Noop implements WatcherSettingsFilter {

        public static Noop INSTANCE = new Noop();

        private Noop() {
        }

        @Override
        public void filterOut(String... patterns) {
        }
    }

    class Shield implements WatcherSettingsFilter {

        private final ShieldIntegration shieldIntegration;

        @Inject
        public Shield(ShieldIntegration shieldIntegration) {
            this.shieldIntegration = shieldIntegration;
        }

        @Override
        public void filterOut(String... patterns) {
            shieldIntegration.filterOutSettings(patterns);
        }
    }
}
