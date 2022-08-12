/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

/**
 * Removes the Elasticsearch Windows service, first stopping it if it is running.
 */
class WindowsServiceRemoveCommand extends ProcrunCommand {
    WindowsServiceRemoveCommand() {
        super("Remove the Elasticsearch Windows Service", "DS");
    }

    @Override
    protected String getSuccessMessage(String serviceId) {
        return "The service '%s' has been removed".formatted(serviceId);
    }

    @Override
    protected String getFailureMessage(String serviceId) {
        return "Failed removing '%s' service".formatted(serviceId);
    }
}
