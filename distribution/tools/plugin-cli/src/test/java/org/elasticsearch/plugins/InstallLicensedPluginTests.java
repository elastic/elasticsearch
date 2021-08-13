/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        InstallPluginAction.checkCanInstallationProceed(terminal, Build.Flavor.OSS, pluginInfo);
    }

    /**
     * Check that a licensed plugin cannot be installed on OSS.
     */
    public void testInstallPluginActionOnOss() throws Exception {
        MockTerminal terminal = new MockTerminal();
        PluginInfo pluginInfo = buildInfo(true);
        final UserException userException = expectThrows(
            UserException.class,
            () -> InstallPluginAction.checkCanInstallationProceed(terminal, Build.Flavor.OSS, pluginInfo)
        );

        assertThat(userException.exitCode, equalTo(ExitCodes.NOPERM));
        assertThat(terminal.getErrorOutput(), containsString("ERROR: This is a licensed plugin"));
    }

    /**
     * Check that a licensed plugin cannot be installed when the distribution type is unknown.
     */
    public void testInstallPluginActionOnUnknownDistribution() throws Exception {
        MockTerminal terminal = new MockTerminal();
        PluginInfo pluginInfo = buildInfo(true);
        expectThrows(
            UserException.class,
            () -> InstallPluginAction.checkCanInstallationProceed(terminal, Build.Flavor.UNKNOWN, pluginInfo)
        );
        assertThat(terminal.getErrorOutput(), containsString("ERROR: This is a licensed plugin"));
    }

    /**
     * Check that a licensed plugin can be installed when the distribution type is default.
     */
    public void testInstallPluginActionOnDefault() throws Exception {
        MockTerminal terminal = new MockTerminal();
        PluginInfo pluginInfo = buildInfo(true);
        InstallPluginAction.checkCanInstallationProceed(terminal, Build.Flavor.DEFAULT, pluginInfo);
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
