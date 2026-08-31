/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest.datafeeds;

import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;

import java.util.Set;

/** REST-handler capability constants for ML datafeed CPS configuration APIs. */
public final class MlDatafeedRestCapabilities {

    /**
     * Signals that put/update/preview datafeed routes accept {@code project_routing} and other ML CPS configuration.
     * Old nodes in a mixed cluster, or nodes with CPS disabled, will not report this capability via {@code /_capabilities}.
     */
    public static final String ML_CROSS_PROJECT_SEARCH = "ml_cross_project_search";

    private static final Set<String> CAPABILITIES_ENABLED = Set.of(ML_CROSS_PROJECT_SEARCH);
    private static final Set<String> CAPABILITIES_DISABLED = Set.of();

    private MlDatafeedRestCapabilities() {}

    public static boolean isMlCrossProjectSearchEnabled(CrossProjectModeDecider crossProjectModeDecider) {
        return DatafeedConfig.isCPSAllowed(crossProjectModeDecider);
    }

    public static Set<String> supportedCapabilities(boolean mlCrossProjectSearchEnabled) {
        return mlCrossProjectSearchEnabled ? CAPABILITIES_ENABLED : CAPABILITIES_DISABLED;
    }
}
