/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import org.elasticsearch.cli.CliToolProvider;
import org.elasticsearch.cli.Command;

public class AutoConfigureNodeToolProvider implements CliToolProvider {
    @Override
    public String name() {
        return "auto-configure-node";
    }

    @Override
    public Command create() {
        return new AutoConfigureNode(false);
    }
}
