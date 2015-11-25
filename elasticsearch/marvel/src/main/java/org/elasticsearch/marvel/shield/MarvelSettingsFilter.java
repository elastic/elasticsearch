/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.shield;

import org.elasticsearch.common.inject.Inject;

/**
 *
 */
public interface MarvelSettingsFilter {

    void filterOut(String... patterns);

    class Noop implements MarvelSettingsFilter {

        public static Noop INSTANCE = new Noop();

        private Noop() {
        }

        @Override
        public void filterOut(String... patterns) {
        }
    }

    class Shield implements MarvelSettingsFilter {

        private final MarvelShieldIntegration shieldIntegration;

        @Inject
        public Shield(MarvelShieldIntegration shieldIntegration) {
            this.shieldIntegration = shieldIntegration;
        }

        @Override
        public void filterOut(String... patterns) {
            shieldIntegration.filterOutSettings(patterns);
        }
    }
}
