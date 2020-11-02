/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.Terminal.Verbosity;
import org.elasticsearch.cli.UserException;

import java.io.IOException;
import java.nio.file.Path;

public class LicensedPlugin {

    static void confirmInstallation(Terminal terminal, Path pluginRoot, boolean isBatch) throws Exception {
        terminal.println(Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        terminal.println(Verbosity.NORMAL, "@     WARNING: You must accept the license to continue    @");
        terminal.println(Verbosity.NORMAL, "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
        terminal.println(Verbosity.NORMAL, "");

        if (isBatch) {
            terminal.println(Verbosity.NORMAL, "This plugin requires that you accept the terms of its license");
            terminal.println(Verbosity.NORMAL, "before continuing with the installation. Since --batch was specified,");
            terminal.println(Verbosity.NORMAL, "installation will assume you accept the license and continue.");
        } else {
            terminal.println(Verbosity.NORMAL, "This plugin requires that you accept the terms of its license");
            terminal.println(Verbosity.NORMAL, "before continuing with the installation. Press <Enter> to view");
            terminal.println(Verbosity.NORMAL, "the license.");

            // Wait for the user to hit enter
            terminal.readText("");
            printLicense(terminal, pluginRoot);

            terminal.println(Verbosity.NORMAL, "");
            final String text = terminal.readText("Continue with installation? [y/N]");
            if (!text.equalsIgnoreCase("y")) {
                throw new UserException(ExitCodes.DATA_ERROR, "installation aborted by user");
            }
        }
    }

    private static void printLicense(Terminal terminal, Path pluginRoot) throws IOException, InterruptedException {
        // Assumption - the license file exists and always has this name.
        final Path licenseFile = pluginRoot.resolve("LICENSE.txt");

        terminal.pageFile(licenseFile);
    }
}
