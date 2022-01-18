/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.xpack.sql.cli.command.CliCommand;
import org.elasticsearch.xpack.sql.cli.command.CliSession;

import java.util.Locale;

public class CliRepl {

    private CliTerminal cliTerminal;
    private CliCommand cliCommand;
    private CliSession cliSession;

    public CliRepl(CliTerminal cliTerminal, CliSession cliSession, CliCommand cliCommand) {
        this.cliTerminal = cliTerminal;
        this.cliCommand = cliCommand;
        this.cliSession = cliSession;
    }

    public void execute() {
        String DEFAULT_PROMPT = "sql> ";
        String MULTI_LINE_PROMPT = "   | ";

        StringBuilder multiLine = new StringBuilder();
        String prompt = DEFAULT_PROMPT;

        cliTerminal.flush();
        cliCommand.handle(cliTerminal, cliSession, "logo");

        while (true) {
            String line = cliTerminal.readLine(prompt);
            if (line == null) {
                return;
            }
            line = line.trim();

            if (line.endsWith(";") == false) {
                multiLine.append(line);
                multiLine.append(" ");
                prompt = MULTI_LINE_PROMPT;
                continue;
            }

            line = line.substring(0, line.length() - 1);

            prompt = DEFAULT_PROMPT;
            if (multiLine.length() > 0) {
                // append the line without trailing ;
                multiLine.append(line);
                line = multiLine.toString().trim();
                multiLine.setLength(0);
            }

            // Skip empty commands
            if (line.isEmpty()) {
                continue;
            }

            // special case to handle exit
            if (isExit(line)) {
                cliTerminal.line().em("Bye!").ln();
                cliTerminal.flush();
                return;
            }
            if (cliCommand.handle(cliTerminal, cliSession, line) == false) {
                cliTerminal.error("Unrecognized command", line);
            }
            cliTerminal.println();
        }
    }

    private static boolean isExit(String line) {
        line = line.toLowerCase(Locale.ROOT);
        return line.equals("exit") || line.equals("quit");
    }

}
