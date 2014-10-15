/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.inject.ImplementedBy;

import static org.elasticsearch.license.plugin.core.LicensesService.TrialLicenseOptions;

@ImplementedBy(LicensesService.class)
public interface LicensesClientService {

    public interface Listener {

        /**
         * Called to enable a feature
         */
        public void onEnabled();

        /**
         * Called to disable a feature
         */
        public void onDisabled();

        /**
         * Trial license specification used to
         * generate a one-time trial license for the feature
         */
        public TrialLicenseOptions trialLicenseOptions();
    }

    /**
     * Registers a feature for licensing
     * @param feature - name of the feature to register (must be in sync with license Generator feature name)
     * @param listener - used to notify on feature enable/disable and specify trial license specification
     */
    void register(String feature, Listener listener);
}
