/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.support.cli;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 */
public class HelpPrinter {

    public void print(CliToolConfig config, Terminal terminal) {
        URL url = config.toolType().getResource(config.name() + ".help");
        print(url, terminal);
    }

    public void print(CliToolConfig.Cmd cmd, Terminal terminal) {
        URL url = cmd.cmdType().getResource(cmd.name() + ".help");
        print(url, terminal);
    }

    private static void print(URL url, Terminal terminal) {
        terminal.println();
        try {
            Path helpFile = Paths.get(url.toURI());
            for (String line : Files.readAllLines(helpFile, Charsets.UTF_8)) {
                terminal.println(line);
            }
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace(terminal.writer());
        }
        terminal.println();
    }
}
