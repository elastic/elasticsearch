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

import org.elasticsearch.Build;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;

import java.util.List;

public class LicensedPlugin {

    static void checkCanInstallationProceed(Terminal terminal, Build.Flavor flavor, PluginInfo info) throws Exception {
        if (info.isLicensed() == false) {
            return;
        }

        if (flavor == Build.Flavor.DEFAULT) {
            return;
        }

        List.of(
            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@",
            "@            ERROR: This is a licensed plugin             @",
            "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@",
            "",
            "This plugin is covered by the Elastic license, but this",
            "installation of Elasticsearch is: [" + flavor + "]."
        ).forEach(terminal::errorPrintln);

        throw new UserException(ExitCodes.NOPERM, "Plugin license is incompatible with [" + flavor + "] installation");
    }
}
