/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.CliTerminal;
import org.elasticsearch.xpack.sql.client.HttpClient;
import org.elasticsearch.xpack.sql.client.JreHttpUrlConnection;
import org.elasticsearch.xpack.sql.proto.Mode;
import org.elasticsearch.xpack.sql.proto.SqlQueryResponse;
import org.elasticsearch.xpack.sql.proto.formatter.SimpleFormatter;

import java.sql.SQLException;

import static org.elasticsearch.xpack.sql.proto.formatter.SimpleFormatter.FormatOption.CLI;

public class ServerQueryCliCommand extends AbstractServerCliCommand {

    @Override
    protected boolean doHandle(CliTerminal terminal, CliSession cliSession, String line) {
        SqlQueryResponse response = null;
        HttpClient cliClient = cliSession.getClient();
        SimpleFormatter formatter;
        String data;
        try {
            response = cliClient.basicQuery(line, cliSession.cfg().getFetchSize(), cliSession.cfg().isLenient());
            formatter = new SimpleFormatter(response.columns(), response.rows(), CLI);
            data = formatter.formatWithHeader(response.columns(), response.rows());
            while (true) {
                handleText(terminal, data);
                if (response.cursor().isEmpty()) {
                    // Successfully finished the entire query!
                    terminal.flush();
                    return true;
                }
                if (false == cliSession.cfg().getFetchSeparator().equals("")) {
                    terminal.println(cliSession.cfg().getFetchSeparator());
                }
                response = cliSession.getClient().nextPage(response.cursor());
                data = formatter.formatWithoutHeader(response.rows());
            }
        } catch (SQLException e) {
            if (JreHttpUrlConnection.SQL_STATE_BAD_SERVER.equals(e.getSQLState())) {
                terminal.error("Server error", e.getMessage());
            } else {
                terminal.error("Bad request", e.getMessage());
            }
            if (response != null) {
                try {
                    cliClient.queryClose(response.cursor(), Mode.CLI);
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
