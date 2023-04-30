/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

/**
 * Runs the procrun GUI manager for the Elasticsearch Windows service.
 */
class WindowsServiceManagerCommand extends ProcrunCommand {
    WindowsServiceManagerCommand() {
        super("Starts the Elasticsearch Windows Service manager", "ES");
    }

    @Override
    protected String getExecutable() {
        return "elasticsearch-service-mgr.exe";
    }

    @Override
    protected boolean includeLogArgs() {
        return false;
    }

    @Override
    protected String getSuccessMessage(String serviceId) {
        return String.format(java.util.Locale.ROOT, "Successfully started service manager for '%s'", serviceId);
    }

    @Override
    protected String getFailureMessage(String serviceId) {
        return String.format(java.util.Locale.ROOT, "Failed starting service manager for '%s'", serviceId);
    }
}
