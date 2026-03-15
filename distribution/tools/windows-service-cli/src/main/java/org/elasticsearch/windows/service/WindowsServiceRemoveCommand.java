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
 * Removes the Elasticsearch Windows service.
 *
 * <p>Uses the SCM {@code DeleteService} API to mark the service for deletion.
 */
class WindowsServiceRemoveCommand extends ScmCommand {

    WindowsServiceRemoveCommand() {
        this(WindowsServiceControl.create());
    }

    WindowsServiceRemoveCommand(WindowsServiceControl serviceControl) {
        super("Remove the Elasticsearch Windows Service", serviceControl);
    }

    @Override
    protected void executeServiceCommand(WindowsServiceControl serviceControl, String serviceId) throws WindowsServiceException {
        serviceControl.deleteService(serviceId);
    }

    @Override
    protected String getSuccessMessage(String serviceId) {
        return String.format(Locale.ROOT, "The service '%s' has been removed", serviceId);
    }

    @Override
    protected String getFailureMessage(String serviceId) {
        return String.format(Locale.ROOT, "Failed removing '%s' service", serviceId);
    }
}
