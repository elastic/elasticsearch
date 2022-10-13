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
 * lenient command, enables/disables fields multi-value leniency.
 * ie. with lenient = true, in case of array values, return the first value, with no guarantee of consistent results.
 *
 */
public class LenientCliCommand extends AbstractCliCommand {

    public LenientCliCommand() {
        super(Pattern.compile("lenient *= *(.+)", Pattern.CASE_INSENSITIVE));
    }

    @Override
    protected boolean doHandle(CliTerminal terminal, CliSession cliSession, Matcher m, String line) {
        cliSession.cfg().setLenient(Boolean.parseBoolean(m.group(1)));
        terminal.line().text("lenient set to ").em(Boolean.toString(cliSession.cfg().isLenient())).end();
        return true;
    }
}
