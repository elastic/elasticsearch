/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.cli;

import org.elasticsearch.Build;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.cli.ExitCodes;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * The helper that {@code bin/elasticsearch-cli} runs before a tool. Its output is a list of JVM options, one per line, so the
 * tests pin the exact output: nothing when FIPS mode is off, and only option lines when it is on.
 */
public class FipsOptionsCommandTests extends CommandTestCase {

    private Build.Type buildType = Build.Type.TAR;

    @Override
    protected Command newCommand() {
        return new FipsOptionsCommand() {
            @Override
            protected Build.Type getBuildType() {
                return buildType;
            }
        };
    }

    private Path shipFipsPayload() throws IOException {
        Path fips = FipsOptions.fipsLib(esHomeDir);
        Files.createDirectories(fips);
        Files.writeString(fips.resolve(FipsOptions.JAVA_SECURITY_FILE), "keystore.type=BCFKS\n");
        return fips;
    }

    private void enableInYml() throws IOException {
        Files.writeString(configDir.resolve("elasticsearch.yml"), FipsOptions.FIPS_MODE_SETTING + ": true\n");
    }

    public void testPrintsNothingWhenNotInFipsMode() throws Exception {
        shipFipsPayload();
        assertOkWithOutput(emptyString(), emptyString());
        Files.writeString(configDir.resolve("elasticsearch.yml"), FipsOptions.FIPS_MODE_SETTING + ": false\n");
        assertOkWithOutput(emptyString(), emptyString());
    }

    public void testPrintsOptionsFromYml() throws Exception {
        Path fips = shipFipsPayload();
        enableInYml();
        String output = execute();
        assertThat(output.lines().count(), equalTo(6L));
        assertThat(output.lines().allMatch(line -> line.startsWith("-D")), is(true));
        assertThat(output, containsString("-Djava.security.properties==" + fips.resolve(FipsOptions.JAVA_SECURITY_FILE) + "\n"));
    }

    public void testPrintsOptionsFromSettingArgument() throws Exception {
        shipFipsPayload();
        String output = execute("-E", FipsOptions.FIPS_MODE_SETTING + "=true");
        assertThat(output.lines().count(), equalTo(6L));
    }

    public void testPrintsOptionsFromDockerEnvironment() throws Exception {
        shipFipsPayload();
        envVars.put("ES_SETTING_XPACK_SECURITY_FIPS__MODE_ENABLED", "true");
        assertOkWithOutput(emptyString(), emptyString());
        buildType = Build.Type.DOCKER;
        assertThat(execute().lines().count(), equalTo(6L));
    }

    public void testFailsWhenTheProviderIsNotShipped() throws Exception {
        enableInYml();
        int status = executeMain();
        assertThat(status, equalTo(ExitCodes.CONFIG));
        assertThat(terminal.getOutput(), emptyString());
        assertThat(terminal.getErrorOutput(), containsString("does not contain the FIPS provider"));
    }
}
