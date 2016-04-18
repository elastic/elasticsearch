/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.graph;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.License.OperationMode;
import org.elasticsearch.license.plugin.core.AbstractLicenseeComponent;
import org.elasticsearch.license.plugin.core.LicenseState;
import org.elasticsearch.license.plugin.core.LicenseeRegistry;

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
            case STANDARD:
            case GOLD:
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

    /**
     * Determine if Graph Exploration should be enabled.
     * <p>
     * Exploration is only disabled when the license has expired or if the mode is not:
     * <ul>
     * <li>{@link OperationMode#PLATINUM}</li>
     * <li>{@link OperationMode#TRIAL}</li>
     * </ul>
     *
     * @return {@code true} as long as the license is valid. Otherwise {@code false}.
     */
    public boolean isAvailable() {
        // status is volatile
        Status localStatus = status;
        OperationMode operationMode = localStatus.getMode();

        boolean licensed = operationMode == OperationMode.TRIAL || operationMode == OperationMode.PLATINUM;

        return licensed && localStatus.getLicenseState() != LicenseState.DISABLED;
    }
}
