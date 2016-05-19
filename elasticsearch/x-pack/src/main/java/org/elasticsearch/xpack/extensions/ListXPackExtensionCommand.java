/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.extensions;

import joptsimple.OptionSet;
import org.elasticsearch.cli.SettingCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.node.internal.InternalSettingsPreparer;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.elasticsearch.cli.Terminal.Verbosity.VERBOSE;
import static org.elasticsearch.xpack.XPackPlugin.resolveXPackExtensionsFile;

/**
 * A command for the extension cli to list extensions installed in x-pack.
 */
class ListXPackExtensionCommand extends SettingCommand {

    ListXPackExtensionCommand() {
        super("Lists installed x-pack extensions");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Map<String, String> settings) throws Exception {
        Environment env = InternalSettingsPreparer.prepareEnvironment(Settings.EMPTY, terminal, settings);
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
