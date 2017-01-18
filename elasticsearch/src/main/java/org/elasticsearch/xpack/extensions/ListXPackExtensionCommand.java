/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import joptsimple.OptionSet;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;
import static org.elasticsearch.xpack.XPackPlugin.resolveXPackExtensionsFile;

/**
 * A command for the extension cli to list extensions installed in x-pack.
 */
class ListXPackExtensionCommand extends EnvironmentAwareCommand {

    ListXPackExtensionCommand() {
        super("Lists installed x-pack extensions");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (Files.exists(resolveXPackExtensionsFile(env)) == false) {
            throw new IOException("Extensions directory missing: " + resolveXPackExtensionsFile(env));
        }
        terminal.println(VERBOSE, "XPack Extensions directory: " + resolveXPackExtensionsFile(env));
        final List<Path> extensions = new ArrayList<>();
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(resolveXPackExtensionsFile(env))) {
            for (Path extension : paths) {
                extensions.add(extension);
            }
        }
        Collections.sort(extensions);
        for (final Path extension : extensions) {
            terminal.println(extension.getFileName().toString());
            XPackExtensionInfo info =
                    XPackExtensionInfo.readFromProperties(extension);
            terminal.println(VERBOSE, info.toString());
        }
    }

}
