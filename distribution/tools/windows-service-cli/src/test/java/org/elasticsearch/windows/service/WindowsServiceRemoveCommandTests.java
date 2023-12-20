/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.windows.service;

import org.elasticsearch.cli.Command;

import java.io.IOException;

public class WindowsServiceRemoveCommandTests extends WindowsServiceCliTestCase {

    public WindowsServiceRemoveCommandTests(boolean spaceInPath) {
        super(spaceInPath);
    }

    @Override
    protected Command newCommand() {
        return new WindowsServiceRemoveCommand() {
            @Override
            Process startProcess(ProcessBuilder processBuilder) throws IOException {
                return mockProcess(processBuilder);
            }
        };
    }

    @Override
    protected String getCommand() {
        return "DS";
    }

    @Override
    protected String getDefaultSuccessMessage() {
        return "The service 'elasticsearch-service-x64' has been removed";
    }

    @Override
    protected String getDefaultFailureMessage() {
        return "Failed removing 'elasticsearch-service-x64' service";
    }
}
