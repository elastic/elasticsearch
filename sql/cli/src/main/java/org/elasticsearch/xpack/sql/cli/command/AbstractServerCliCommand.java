/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.CliTerminal;

public abstract class AbstractServerCliCommand implements CliCommand {

    public AbstractServerCliCommand() {
    }

    @Override
    public final boolean handle(CliTerminal terminal, CliSession cliSession, String line) {
        try {
            return doHandle(terminal, cliSession, line);
        } catch (RuntimeException e) {
            handleExceptionWhileCommunicatingWithServer(terminal, cliSession, e);
        }
        return true;
    }

    protected abstract boolean doHandle(CliTerminal cliTerminal, CliSession cliSession, String line);

    /**
     * Handle an exception while communication with the server. Extracted
     * into a method so that tests can bubble the failure.
     */
    protected void handleExceptionWhileCommunicatingWithServer(CliTerminal terminal, CliSession cliSession, RuntimeException e) {
        terminal.line().error("Communication error [").param(e.getMessage() == null ? e.getClass().getName() : e.getMessage()).error("]")
                .ln();
        if (cliSession.isDebug()) {
            terminal.printStackTrace(e);
        }
    }


}
