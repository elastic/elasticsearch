/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

/**
 * Stops the Elasticsearch Windows service.
 */
class WindowsServiceStopCommand extends ProcrunCommand {
    WindowsServiceStopCommand() {
        super("Stops the Elasticsearch Windows Service", "SS");
    }

    @Override
    protected String getSuccessMessage(String serviceId) {
        return String.format(java.util.Locale.ROOT, "The service '%s' has been stopped", serviceId);
    }

    @Override
    protected String getFailureMessage(String serviceId) {
        return String.format(java.util.Locale.ROOT, "Failed stopping '%s' service", serviceId);
    }
}
