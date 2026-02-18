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

public class WindowsServiceRemoveCommandTests extends ScmCommandTestCase {

    public WindowsServiceRemoveCommandTests(boolean spaceInPath) {
        super(spaceInPath);
    }

    @Override
    protected Command newCommand() {
        return new WindowsServiceRemoveCommand(mockServiceControl);
    }

    @Override
    protected String getExpectedOperation() {
        return "delete";
    }

    @Override
    protected String getDefaultSuccessMessage() {
        return "The service 'elasticsearch-service-x64' has been removed";
    }

    @Override
    protected String getDefaultFailureMessage() {
        return "Failed removing 'elasticsearch-service-x64' service";
    }

    public void testFailure() throws Exception {
        mockServiceControl.deleteException = new WindowsServiceException("Service does not exist", 1060);
        int exitCode = executeMain();
        assertThat(exitCode, equalTo(ExitCodes.CODE_ERROR));
        assertThat(terminal.getErrorOutput(), containsString(getDefaultFailureMessage()));
        assertThat(terminal.getErrorOutput(), containsString("Service does not exist"));
    }
}
