/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.CliTerminal;

public interface CliCommand {

    /**
     * Handle the command, return true if the command is handled, false otherwise
     */
    boolean handle(CliTerminal terminal, CliSession cliSession, String line);

}
