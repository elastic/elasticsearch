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
 * The base class for simple commands that match the pattern
 */
public abstract class AbstractCliCommand implements CliCommand {

    protected final Pattern pattern;

    AbstractCliCommand(Pattern pattern) {
        this.pattern = pattern;
    }

    @Override
    public boolean handle(CliTerminal terminal, CliSession cliSession, String line) {
        Matcher matcher = pattern.matcher(line);
        if (matcher.matches()) {
            return doHandle(terminal, cliSession, matcher, line);
        }
        return false;
    }

    /**
     * the perform the command
     * returns true if the command handled the line and false otherwise
     */
    protected abstract boolean doHandle(CliTerminal terminal, CliSession cliSession, Matcher m, String line);
}
