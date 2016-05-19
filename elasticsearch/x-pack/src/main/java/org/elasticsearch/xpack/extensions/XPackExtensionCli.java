/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;
import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.Terminal;

/**
 * A cli tool for adding, removing and listing extensions for x-pack.
 */
public class XPackExtensionCli extends MultiCommand {

    public XPackExtensionCli() {
        super("A tool for managing installed x-pack extensions");
        subcommands.put("list", new ListXPackExtensionCommand());
        subcommands.put("install", new InstallXPackExtensionCommand());
        subcommands.put("remove", new RemoveXPackExtensionCommand());
    }

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure(new NullAppender());
        exit(new XPackExtensionCli().main(args, Terminal.DEFAULT));
    }

}
