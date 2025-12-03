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
 * allows to set project routing for cross-project search
 */
public class ProjectRoutingCliCommand extends AbstractCliCommand {

    public ProjectRoutingCliCommand() {
        super(Pattern.compile("project(?: |_)routing(?: |_) *= *(.+)", Pattern.CASE_INSENSITIVE));
    }

    @Override
    protected boolean doHandle(CliTerminal terminal, CliSession cliSession, Matcher m, String line) {
        cliSession.cfg().setProjectRouting(m.group(1));
        terminal.line().text("project_routing set to ").em(cliSession.cfg().projectRouting()).end();
        return true;
    }
}
