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
 * allows to enable/disable return of partial results.
 * If true, returns partial results if there are shard request timeouts or shard failures.
 * If false, returns an error with no partial results.
 *
 */
public class AllowPartialResultsCliCommand extends AbstractCliCommand {

    public AllowPartialResultsCliCommand() {
        super(Pattern.compile("allow(?: |_)partial(?: |_)search(?: |_)results *= *(.+)", Pattern.CASE_INSENSITIVE));
    }

    @Override
    protected boolean doHandle(CliTerminal terminal, CliSession cliSession, Matcher m, String line) {
        cliSession.cfg().setAllowPartialResults(Boolean.parseBoolean(m.group(1)));
        terminal.line().text("allow_partial_search_results set to ").em(Boolean.toString(cliSession.cfg().allowPartialResults())).end();
        return true;
    }
}
