/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.Version;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.MockTerminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class InstallLicensedPluginTests extends ESTestCase {

    /**
     * Check that an unlicensed plugin is accepted.
     */
    public void testUnlicensedPlugin() throws Exception {
        MockTerminal terminal = new MockTerminal();
        PluginInfo pluginInfo = buildInfo(false);
        InstallPluginCommand.checkCanInstallationProceed(terminal, Build.Flavor.OSS, pluginInfo);
    }

    /**
     * Check that a licensed plugin cannot be installed on OSS.
     */
    public void testInstallPluginCommandOnOss() throws Exception {
        MockTerminal terminal = new MockTerminal();
        PluginInfo pluginInfo = buildInfo(true);
        final UserException userException = expectThrows(
            UserException.class,
            () -> InstallPluginCommand.checkCanInstallationProceed(terminal, Build.Flavor.OSS, pluginInfo)
        );

        assertThat(userException.exitCode, equalTo(ExitCodes.NOPERM));
        assertThat(terminal.getErrorOutput(), containsString("ERROR: This is a licensed plugin"));
    }

    /**
     * Check that a licensed plugin cannot be installed when the distribution type is unknown.
     */
    public void testInstallPluginCommandOnUnknownDistribution() throws Exception {
        MockTerminal terminal = new MockTerminal();
        PluginInfo pluginInfo = buildInfo(true);
        expectThrows(
            UserException.class,
            () -> InstallPluginCommand.checkCanInstallationProceed(terminal, Build.Flavor.UNKNOWN, pluginInfo)
        );
        assertThat(terminal.getErrorOutput(), containsString("ERROR: This is a licensed plugin"));
    }

    /**
     * Check that a licensed plugin can be installed when the distribution type is default.
     */
    public void testInstallPluginCommandOnDefault() throws Exception {
        MockTerminal terminal = new MockTerminal();
        PluginInfo pluginInfo = buildInfo(true);
        InstallPluginCommand.checkCanInstallationProceed(terminal, Build.Flavor.DEFAULT, pluginInfo);
    }

    private PluginInfo buildInfo(boolean isLicensed) {
        return new PluginInfo(
            "name",
            "description",
            "version",
            Version.CURRENT,
            "java version",
            "classname",
            List.of(),
            false,
            PluginType.ISOLATED,
            "",
            isLicensed
        );
    }
}
