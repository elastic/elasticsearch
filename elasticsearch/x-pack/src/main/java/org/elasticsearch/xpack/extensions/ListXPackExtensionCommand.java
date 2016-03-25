/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import joptsimple.OptionSet;

import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.xpack.XPackPlugin.resolveXPackExtensionsFile;
import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;

/**
 * A command for the extension cli to list extensions installed in x-pack.
 */
class ListXPackExtensionCommand extends Command {
    private final Environment env;

    ListXPackExtensionCommand(Environment env) {
        super("Lists installed x-pack extensions");
        this.env = env;
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        if (Files.exists(resolveXPackExtensionsFile(env)) == false) {
            throw new IOException("Extensions directory missing: " + resolveXPackExtensionsFile(env));
        }

        terminal.println(VERBOSE, "Extensions directory: " + resolveXPackExtensionsFile(env));
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(resolveXPackExtensionsFile(env))) {
            for (Path extension : stream) {
                terminal.println(extension.getFileName().toString());
            }
        }
    }
}
