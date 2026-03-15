/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.windows.service;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.service.windows.WindowsServiceException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class WindowsServiceStartCommandTests extends ScmCommandTestCase {

    public WindowsServiceStartCommandTests(boolean spaceInPath) {
        super(spaceInPath);
    }

    @Override
    protected Command newCommand() {
        return new WindowsServiceStartCommand(mockServiceControl);
    }

    @Override
    protected String getExpectedOperation() {
        return "start";
    }

    @Override
    protected String getDefaultSuccessMessage() {
        return "The service 'elasticsearch-service-x64' has been started";
    }

    @Override
    protected String getDefaultFailureMessage() {
        return "Failed starting 'elasticsearch-service-x64' service";
    }

    public void testFailure() throws Exception {
        mockServiceControl.startException = new WindowsServiceException("Access denied", 5);
        int exitCode = executeMain();
        assertThat(exitCode, equalTo(ExitCodes.CODE_ERROR));
        assertThat(terminal.getErrorOutput(), containsString(getDefaultFailureMessage()));
        assertThat(terminal.getErrorOutput(), containsString("Access denied"));
    }
}
