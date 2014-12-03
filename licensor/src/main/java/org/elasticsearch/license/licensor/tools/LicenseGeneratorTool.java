/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.cli.commons.CommandLine;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.Licenses;
import org.elasticsearch.license.licensor.LicenseSigner;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;
import static org.elasticsearch.common.cli.CliToolConfig.config;

public class LicenseGeneratorTool extends CliTool {
    public static final String NAME = "license-generator";

    private static final CliToolConfig CONFIG = config("licensor", LicenseGeneratorTool.class)
            .cmds(LicenseGenerator.CMD)
            .build();

    public LicenseGeneratorTool() {
        super(CONFIG);
    }

    @Override
    protected Command parse(String s, CommandLine commandLine) throws Exception {
        return LicenseGenerator.parse(terminal, commandLine);
    }

    public static class LicenseGenerator extends Command {

        private static final CliToolConfig.Cmd CMD = cmd(NAME, LicenseGenerator.class)
                .options(
                        option("pub", "publicKeyPath").required(true).hasArg(true),
                        option("pri", "privateKeyPath").required(true).hasArg(true),
                        option("l", "license").required(false).hasArg(true),
                        option("lf", "licenseFile").required(false).hasArg(true)
                ).build();

        public final Set<License> licenseSpecs;
        public final Path publicKeyFilePath;
        public final Path privateKeyFilePath;

        public LicenseGenerator(Terminal terminal, Path publicKeyFilePath, Path privateKeyFilePath, Set<License> licenseSpecs) {
            super(terminal);
            this.licenseSpecs = licenseSpecs;
            this.privateKeyFilePath = privateKeyFilePath;
            this.publicKeyFilePath = publicKeyFilePath;
        }

        public static Command parse(Terminal terminal, CommandLine commandLine) throws IOException {
            Path publicKeyPath = Paths.get(commandLine.getOptionValue("publicKeyPath"));
            Path privateKeyPath = Paths.get(commandLine.getOptionValue("privateKeyPath"));
            String[] licenseSpecSources = commandLine.getOptionValues("license");
            String[] licenseSpecSourceFiles = commandLine.getOptionValues("licenseFile");

            if (!Files.exists(privateKeyPath)) {
                return exitCmd(ExitStatus.USAGE, terminal, privateKeyPath + " does not exist");
            } else if (!Files.exists(publicKeyPath)) {
                return exitCmd(ExitStatus.USAGE, terminal, publicKeyPath + " does not exist");
            }

            Set<License> licenseSpecs = new HashSet<>();
            if (licenseSpecSources != null) {
                for (String licenseSpec : licenseSpecSources) {
                    licenseSpecs.addAll(Licenses.fromSource(licenseSpec.getBytes(StandardCharsets.UTF_8), false));
                }
            }

            if (licenseSpecSourceFiles != null) {
                for (String licenseSpecFilePath : licenseSpecSourceFiles) {
                    Path licenseSpecPath = Paths.get(licenseSpecFilePath);
                    if (!Files.exists(licenseSpecPath)) {
                        return exitCmd(ExitStatus.USAGE, terminal, licenseSpecFilePath + " does not exist");
                    }
                    licenseSpecs.addAll(Licenses.fromSource(Files.readAllBytes(licenseSpecPath), false));
                }
            }

            if (licenseSpecs.size() == 0) {
                return exitCmd(ExitStatus.USAGE, terminal, "no license spec provided");
            }
            return new LicenseGenerator(terminal, publicKeyPath, privateKeyPath, licenseSpecs);
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {

            // sign
            ImmutableSet<License> signedLicences = new LicenseSigner(privateKeyFilePath, publicKeyFilePath).sign(licenseSpecs);

            // dump
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            Licenses.toXContent(signedLicences, builder, ToXContent.EMPTY_PARAMS);
            builder.flush();
            terminal.print(builder.string());

            return ExitStatus.OK;
        }
    }

    public static void main(String[] args) throws Exception {
        int status = new LicenseGeneratorTool().execute(args);
        System.exit(status);
    }
}
