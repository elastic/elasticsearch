/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.cli;

import org.elasticsearch.cli.UserException;
import org.elasticsearch.xpack.sql.client.shared.ConnectionConfiguration;

import java.net.URI;
import java.util.Properties;

import static org.elasticsearch.xpack.sql.client.shared.UriUtils.parseURI;
import static org.elasticsearch.xpack.sql.client.shared.UriUtils.removeQuery;

/**
 * Connection Builder. Can interactively ask users for the password if it is not provided
 */
public class ConnectionBuilder {
    public static String DEFAULT_CONNECTION_STRING = "http://localhost:9200/";
    public static URI DEFAULT_URI = URI.create(DEFAULT_CONNECTION_STRING);

    private CliTerminal cliTerminal;

    public ConnectionBuilder(CliTerminal cliTerminal) {
        this.cliTerminal = cliTerminal;
    }

    public ConnectionConfiguration buildConnection(String arg) throws UserException {
        final URI uri;
        final String connectionString;
        Properties properties = new Properties();
        String user = null;
        String password = null;
        if (arg != null) {
            connectionString = arg;
            uri = removeQuery(parseURI(connectionString, DEFAULT_URI), connectionString, DEFAULT_URI);
            user = uri.getUserInfo();
            if (user != null) {
                int colonIndex = user.indexOf(':');
                if (colonIndex >= 0) {
                    password = user.substring(colonIndex + 1);
                    user = user.substring(0, colonIndex);
                }
            }
        } else {
            uri = DEFAULT_URI;
            connectionString = DEFAULT_CONNECTION_STRING;
        }

        if (user != null) {
            if (password == null) {
                password = cliTerminal.readPassword("password: ");
            }
            properties.setProperty(ConnectionConfiguration.AUTH_USER, user);
            properties.setProperty(ConnectionConfiguration.AUTH_PASS, password);
        }

        return new ConnectionConfiguration(uri, connectionString, properties);
    }

}
