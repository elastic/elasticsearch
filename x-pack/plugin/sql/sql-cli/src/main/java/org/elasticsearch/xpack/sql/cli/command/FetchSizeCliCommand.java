/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli.command;

import org.elasticsearch.xpack.sql.cli.CliTerminal;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * fetch_size command that allows to change the size of fetches
 */
public class FetchSizeCliCommand extends AbstractCliCommand {

    public FetchSizeCliCommand() {
        super(Pattern.compile("fetch(?: |_)size *= *(.+)", Pattern.CASE_INSENSITIVE));
    }

    @Override
    protected boolean doHandle(CliTerminal terminal, CliSession cliSession, Matcher m, String line) {
        try {
            cliSession.cfg().setFetchSize(Integer.parseInt(m.group(1)));
        } catch (NumberFormatException e) {
            terminal.line().error("Invalid fetch size [").param(m.group(1)).error("]").end();
            return true;
        } catch (IllegalArgumentException e) {
            terminal.line().error("Invalid fetch size [").param(m.group(1)).error("]. " + e.getMessage()).end();
            return true;
        }
        terminal.line().text("fetch size set to ").em(Integer.toString(cliSession.cfg().getFetchSize())).end();
        return true;
    }
}
