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
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;
import org.elasticsearch.graph.Graph;

import static org.elasticsearch.license.core.License.OperationMode.TRIAL;
import static org.elasticsearch.license.core.License.OperationMode.PLATINUM;;

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
        switch (newLicense.operationMode()) {
            case BASIC:
                if (currentLicense != null) {
                    switch (currentLicense.operationMode()) {
                        case TRIAL:
                        case PLATINUM:
                            return new String[] { "Graph will be disabled" };
                    }
                }
                break;
        }
        return Strings.EMPTY_ARRAY;
    }


    public boolean isGraphExploreAllowed() {     
        Status localStatus = status;
        boolean isLicenseStateActive = localStatus.getLicenseState() != LicenseState.DISABLED;
        License.OperationMode operationMode = localStatus.getMode();
        return isLicenseStateActive && (operationMode == TRIAL || operationMode == PLATINUM);
    }
}
