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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.core.LicenseVerifier;
import org.elasticsearch.license.core.Licenses;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.cli.CliToolConfig.Builder.cmd;
import static org.elasticsearch.common.cli.CliToolConfig.Builder.option;
import static org.elasticsearch.common.cli.CliToolConfig.config;

public class LicenseVerificationTool extends CliTool {
    public static final String NAME = "verify-license";

    private static final CliToolConfig CONFIG = config("licensor", LicenseVerificationTool.class)
            .cmds(LicenseVerifier.CMD)
            .build();

    public LicenseVerificationTool() {
        super(CONFIG);
    }

    @Override
    protected Command parse(String s, CommandLine commandLine) throws Exception {
        return LicenseVerifier.parse(terminal, commandLine);
    }

    public static class LicenseVerifier extends Command {

        private static final CliToolConfig.Cmd CMD = cmd(NAME, LicenseVerifier.class)
                .options(
                        option("pub", "publicKeyPath").required(true).hasArg(true),
                        option("l", "license").required(false).hasArg(true),
                        option("lf", "licenseFile").required(false).hasArg(true)
                ).build();

        public final Set<License> licenses;
        public final Path publicKeyPath;

        public LicenseVerifier(Terminal terminal, Set<License> licenses, Path publicKeyPath) {
            super(terminal);
            this.licenses = licenses;
            this.publicKeyPath = publicKeyPath;
        }

        public static Command parse(Terminal terminal, CommandLine commandLine) throws IOException {
            String publicKeyPathString = commandLine.getOptionValue("publicKeyPath");
            String[] licenseSources = commandLine.getOptionValues("license");
            String[] licenseSourceFiles = commandLine.getOptionValues("licenseFile");

            Set<License> licenses = new HashSet<>();
            if (licenseSources != null) {
                for (String licenseSpec : licenseSources) {
                    licenses.addAll(Licenses.fromSource(licenseSpec.getBytes(StandardCharsets.UTF_8)));
                }
            }

            if (licenseSourceFiles != null) {
                for (String licenseFilePath : licenseSourceFiles) {
                    Path licensePath = Paths.get(licenseFilePath);
                    if (!Files.exists(licensePath)) {
                        return exitCmd(ExitStatus.USAGE, terminal, licenseFilePath + " does not exist");
                    }
                    licenses.addAll(Licenses.fromSource(Files.readAllBytes(licensePath)));
                }
            }

            if (licenses.size() == 0) {
                return exitCmd(ExitStatus.USAGE, terminal, "no license provided");
            }

            Path publicKeyPath = Paths.get(publicKeyPathString);
            if (!Files.exists(publicKeyPath)) {
                return exitCmd(ExitStatus.USAGE, terminal, publicKeyPath + " does not exist");
            }

            return new LicenseVerifier(terminal, licenses, publicKeyPath);
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {

            // verify
            Map<String, License> effectiveLicenses = Licenses.reduceAndMap(licenses);

            if (!org.elasticsearch.license.core.LicenseVerifier.verifyLicenses(effectiveLicenses.values(), publicKeyPath)) {
                terminal.println("Invalid License(s)!");
                return ExitStatus.DATA_ERROR;
            }

            // dump effective licences
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            Licenses.toXContent(effectiveLicenses.values(), builder, ToXContent.EMPTY_PARAMS);
            builder.flush();
            terminal.print(builder.string());

            return ExitStatus.OK;
        }
    }

    public static void main(String[] args) throws Exception {
        int status = new LicenseVerificationTool().execute(args);
        System.exit(status);
    }
}
