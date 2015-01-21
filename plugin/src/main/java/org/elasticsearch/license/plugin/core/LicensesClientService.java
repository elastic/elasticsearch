/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.common.inject.ImplementedBy;
import org.elasticsearch.license.core.License;

import java.util.Collection;

import static org.elasticsearch.license.plugin.core.LicensesService.*;
import static org.elasticsearch.license.plugin.core.LicensesService.TrialLicenseOptions;

@ImplementedBy(LicensesService.class)
public interface LicensesClientService {

    public interface Listener {

        /**
         * Called to enable a feature
         */
        public void onEnabled(License license);

        /**
         * Called to disable a feature
         */
        public void onDisabled(License license);

    }

    /**
     * Registers a feature for licensing
     *
     * @param feature             - name of the feature to register (must be in sync with license Generator feature name)
     * @param trialLicenseOptions - Trial license specification used to generate a one-time trial license for the feature;
     *                            use <code>null</code> if no trial license should be generated for the feature
     * @param expirationCallbacks - A collection of Pre and/or Post expiration callbacks
     * @param listener            - used to notify on feature enable/disable
     */
    void register(String feature, TrialLicenseOptions trialLicenseOptions, Collection<ExpirationCallback> expirationCallbacks, Listener listener);
}
