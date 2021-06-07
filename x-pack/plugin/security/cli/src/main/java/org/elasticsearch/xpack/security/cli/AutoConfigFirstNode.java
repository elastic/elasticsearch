/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import joptsimple.OptionSet;
import org.elasticsearch.cli.LoggingAwareCommand;
import org.elasticsearch.cli.Terminal;

public class AutoConfigFirstNode extends LoggingAwareCommand {

    public AutoConfigFirstNode() {
        super("Generates all the necessary configuration for the first node of a new secure cluster");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
    }
}
