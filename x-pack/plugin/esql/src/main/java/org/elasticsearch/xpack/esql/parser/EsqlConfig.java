/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser;

import org.elasticsearch.Build;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;

class EsqlConfig {

    // versioning information
    boolean devVersion = Build.current().isSnapshot();

    boolean metricsCommand = EsqlCapabilities.Cap.METRICS_COMMAND.isEnabled();

    public boolean isDevVersion() {
        return devVersion;
    }

    boolean isReleaseVersion() {
        return isDevVersion() == false;
    }

    public void setDevVersion(boolean dev) {
        this.devVersion = dev;
    }

    public boolean hasMetricsCommand() {
        return metricsCommand;
    }

    public void setMetricsCommand(boolean metricsCommand) {
        this.metricsCommand = metricsCommand;
    }
}
