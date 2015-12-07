/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.apache.commons.cli.CommandLine;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.cli.CliTool;
import org.elasticsearch.common.cli.CliToolConfig;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.core.License;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
        return LicenseVerifier.parse(terminal, commandLine, env);
    }

    public static class LicenseVerifier extends Command {

        private static final CliToolConfig.Cmd CMD = cmd(NAME, LicenseVerifier.class)
                .options(
                        option("pub", "publicKeyPath").required(true).hasArg(true),
                        option("l", "license").required(false).hasArg(true),
                        option("lf", "licenseFile").required(false).hasArg(true)
                ).build();

        public final License license;
        public final Path publicKeyPath;

        public LicenseVerifier(Terminal terminal, License license, Path publicKeyPath) {
            super(terminal);
            this.license = license;
            this.publicKeyPath = publicKeyPath;
        }

        public static Command parse(Terminal terminal, CommandLine commandLine, Environment environment) throws IOException {
            String publicKeyPathString = commandLine.getOptionValue("publicKeyPath");
            String licenseSource = commandLine.getOptionValue("license");
            String licenseSourceFile = commandLine.getOptionValue("licenseFile");

            License license = null;
            if (licenseSource != null) {
                license = License.fromSource(licenseSource);
            } else if (licenseSourceFile != null) {
                Path licenseSpecPath = environment.binFile().getParent().resolve(licenseSourceFile);
                if (!Files.exists(licenseSpecPath)) {
                    return exitCmd(ExitStatus.USAGE, terminal, licenseSourceFile + " does not exist");
                }
                license = License.fromSource(Files.readAllBytes(licenseSpecPath));
            }
            if (license == null) {
                return exitCmd(ExitStatus.USAGE, terminal, "no license spec provided");
            }
            Path publicKeyPath = environment.binFile().getParent().resolve(publicKeyPathString);
            if (!Files.exists(publicKeyPath)) {
                return exitCmd(ExitStatus.USAGE, terminal, publicKeyPath + " does not exist");
            }
            return new LicenseVerifier(terminal, license, publicKeyPath);
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {

            // verify
            if (!org.elasticsearch.license.core.LicenseVerifier.verifyLicense(license, Files.readAllBytes(publicKeyPath))) {
                terminal.println("Invalid License!");
                return ExitStatus.DATA_ERROR;
            }
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            builder.startObject("license");
            license.toInnerXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            builder.endObject();
            builder.flush();
            terminal.print(builder.string());
            return ExitStatus.OK;
        }
    }

    public static void main(String[] args) throws Exception {
        ExitStatus exitStatus  = new LicenseVerificationTool().execute(args);
        exit(exitStatus.status());
    }

    @SuppressForbidden(reason = "Allowed to exit explicitly from #main()")
    private static void exit(int status) {
        System.exit(status);
    }
}
