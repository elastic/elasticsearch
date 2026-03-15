/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.windows.service;

import org.elasticsearch.service.windows.WindowsServiceControl;
import org.elasticsearch.service.windows.WindowsServiceException;

import java.util.Locale;

/**
 * Starts the Elasticsearch Windows service.
 */
class WindowsServiceStartCommand extends ScmCommand {

    WindowsServiceStartCommand() {
        this(WindowsServiceControl.create());
    }

    WindowsServiceStartCommand(WindowsServiceControl serviceControl) {
        super("Starts the Elasticsearch Windows Service", serviceControl);
    }

    @Override
    protected void executeServiceCommand(WindowsServiceControl serviceControl, String serviceId) throws WindowsServiceException {
        serviceControl.startService(serviceId);
    }

    @Override
    protected String getSuccessMessage(String serviceId) {
        return String.format(Locale.ROOT, "The service '%s' has been started", serviceId);
    }

    @Override
    protected String getFailureMessage(String serviceId) {
        return String.format(Locale.ROOT, "Failed starting '%s' service", serviceId);
    }
}
