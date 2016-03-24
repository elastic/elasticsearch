/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.graph.license;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.AbstractLicenseeComponent;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.graph.Graph;

public class GraphLicensee extends AbstractLicenseeComponent<GraphLicensee> {

    public static final String ID = Graph.NAME;

    @Inject
    public GraphLicensee(Settings settings, LicenseeRegistry clientService) {
        super(settings, ID, clientService);
    }

    @Override
    public String[] expirationMessages() {
        return new String[] {
                "Graph explore APIs are disabled"
        };
    }

    @Override
    public String[] acknowledgmentMessages(License currentLicense, License newLicense) {
        if (newLicense.operationMode().allFeaturesEnabled() == false) {
            if (currentLicense != null && currentLicense.operationMode().allFeaturesEnabled()) {
                return new String[] { "Graph will be disabled" };
            }
        }
        return Strings.EMPTY_ARRAY;
    }

    public boolean isGraphExploreEnabled() {
        // status is volatile
        Status localStatus = status;
        return localStatus.getLicenseState().isActive() && localStatus.getMode().allFeaturesEnabled();
    }
}
