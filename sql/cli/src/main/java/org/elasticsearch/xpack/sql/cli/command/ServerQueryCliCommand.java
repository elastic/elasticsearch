/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.CliTerminal;
import org.elasticsearch.xpack.sql.cli.net.protocol.QueryResponse;
import org.elasticsearch.xpack.sql.client.shared.JreHttpUrlConnection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;

public class ServerQueryCliCommand extends AbstractServerCliCommand {

    @Override
    protected boolean doHandle(CliTerminal terminal, CliSession cliSession, String line) {
        QueryResponse response;
        try {
            response = cliSession.getClient().queryInit(line, cliSession.getFetchSize());
        } catch (SQLException e) {
            if (JreHttpUrlConnection.SQL_STATE_BAD_SERVER.equals(e.getSQLState())) {
                terminal.error("Server error", e.getMessage());
            } else {
                terminal.error("Bad request", e.getMessage());
            }
            return true;
        }
        if (response.data.startsWith("digraph ")) {
            handleGraphviz(terminal, response.data);
            return true;
        }
        while (true) {
            handleText(terminal, response.data);
            if (response.cursor().isEmpty()) {
                // Successfully finished the entire query!
                terminal.flush();
                return true;
            }
            if (false == cliSession.getFetchSeparator().equals("")) {
                terminal.println(cliSession.getFetchSeparator());
            }
            try {
                response = cliSession.getClient().nextPage(response.cursor());
            } catch (SQLException e) {
                if (JreHttpUrlConnection.SQL_STATE_BAD_SERVER.equals(e.getSQLState())) {
                    terminal.error("Server error", e.getMessage());
                } else {
                    terminal.error("Bad request", e.getMessage());
                }
                return true;
            }
        }
    }

    private void handleText(CliTerminal terminal, String str) {
        terminal.print(str);
    }

    private void handleGraphviz(CliTerminal terminal, String str) {
        try {
            // save the content to a temp file
            Path dotTempFile = Files.createTempFile(Paths.get("."), "sql-gv", ".dot");
            Files.write(dotTempFile, str.getBytes(StandardCharsets.UTF_8));
            terminal.println("Saved graph file at " + dotTempFile);
        } catch (IOException ex) {
            terminal.error("Cannot save graph file ", ex.getMessage());
        }
    }

}
