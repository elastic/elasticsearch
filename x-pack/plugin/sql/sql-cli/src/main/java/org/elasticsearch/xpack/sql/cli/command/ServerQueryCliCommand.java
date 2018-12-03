/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.action.CliFormatter;
import org.elasticsearch.xpack.sql.cli.CliTerminal;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.client.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.proto.SqlQueryResponse;

import java.sql.SQLException;

public class ServerQueryCliCommand extends AbstractServerCliCommand {

    @Override
    protected boolean doHandle(CliTerminal terminal, CliSession cliSession, String line) {
        SqlQueryResponse response = null;
        HttpClient cliClient = cliSession.getClient();
        CliFormatter cliFormatter;
        String data;
        try {
            response = cliClient.queryInit(line, cliSession.getFetchSize());
            cliFormatter = new CliFormatter(response.columns(), response.rows());
            data = cliFormatter.formatWithHeader(response.columns(), response.rows());
            while (true) {
                handleText(terminal, data);
                if (response.cursor().isEmpty()) {
                    // Successfully finished the entire query!
                    terminal.flush();
                    return true;
                }
                if (false == cliSession.getFetchSeparator().equals("")) {
                    terminal.println(cliSession.getFetchSeparator());
                }
                response = cliSession.getClient().nextPage(response.cursor());
                data = cliFormatter.formatWithoutHeader(response.rows());
            }
        } catch (SQLException e) {
            if (JreHttpUrlConnection.SQL_STATE_BAD_SERVER.equals(e.getSQLState())) {
                terminal.error("Server error", e.getMessage());
            } else {
                terminal.error("Bad request", e.getMessage());
            }
            if (response != null) {
                try {
                    cliClient.queryClose(response.cursor());
                } catch (SQLException ex) {
                    terminal.error("Could not close cursor", ex.getMessage());
                }
            }
        }
        return true;
    }

    private void handleText(CliTerminal terminal, String str) {
        terminal.print(str);
    }
}
