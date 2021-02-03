/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.CliTerminal;
import org.elasticsearch.xpack.sql.proto.MainResponse;

import java.sql.SQLException;
import java.util.Locale;

public class ServerInfoCliCommand extends AbstractServerCliCommand {

    public ServerInfoCliCommand() {
    }

    @Override
    public boolean doHandle(CliTerminal terminal, CliSession cliSession, String line) {
        if (false == "info".equals(line.toLowerCase(Locale.ROOT))) {
            return false;
        }
        MainResponse info;
        try {
            info = cliSession.getClient().serverInfo();
        } catch (SQLException e) {
            terminal.error("Error fetching server info", e.getMessage());
            return true;
        }
        terminal.line()
                .text("Node:").em(info.getNodeName())
                .text(" Cluster:").em(info.getClusterName())
                .text(" Version:").em(info.getVersion())
                .ln();
        return true;
    }
}
