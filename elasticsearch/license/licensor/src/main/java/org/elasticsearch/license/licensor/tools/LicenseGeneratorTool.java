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
import org.elasticsearch.license.licensor.LicenseSigner;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
        return LicenseGenerator.parse(terminal, commandLine, env);
    }

    public static class LicenseGenerator extends Command {

        private static final CliToolConfig.Cmd CMD = cmd(NAME, LicenseGenerator.class)
                .options(
                        option("pub", "publicKeyPath").required(true).hasArg(true),
                        option("pri", "privateKeyPath").required(true).hasArg(true),
                        option("l", "license").required(false).hasArg(true),
                        option("lf", "licenseFile").required(false).hasArg(true)
                ).build();

        public final License licenseSpec;
        public final Path publicKeyFilePath;
        public final Path privateKeyFilePath;

        public LicenseGenerator(Terminal terminal, Path publicKeyFilePath, Path privateKeyFilePath, License licenseSpec) {
            super(terminal);
            this.licenseSpec = licenseSpec;
            this.privateKeyFilePath = privateKeyFilePath;
            this.publicKeyFilePath = publicKeyFilePath;
        }

        public static Command parse(Terminal terminal, CommandLine commandLine, Environment environment) throws IOException {
            Path publicKeyPath = environment.binFile().getParent().resolve(commandLine.getOptionValue("publicKeyPath"));
            Path privateKeyPath = environment.binFile().getParent().resolve(commandLine.getOptionValue("privateKeyPath"));
            String licenseSpecSource = commandLine.getOptionValue("license");
            String licenseSpecSourceFile = commandLine.getOptionValue("licenseFile");

            if (!Files.exists(privateKeyPath)) {
                return exitCmd(ExitStatus.USAGE, terminal, privateKeyPath + " does not exist");
            } else if (!Files.exists(publicKeyPath)) {
                return exitCmd(ExitStatus.USAGE, terminal, publicKeyPath + " does not exist");
            }

            License license = null;
            if (licenseSpecSource != null) {
                 license = License.fromSource(licenseSpecSource);
            } else if (licenseSpecSourceFile != null) {
                Path licenseSpecPath = environment.binFile().getParent().resolve(licenseSpecSourceFile);
                if (!Files.exists(licenseSpecPath)) {
                    return exitCmd(ExitStatus.USAGE, terminal, licenseSpecSourceFile + " does not exist");
                }
                license = License.fromSource(Files.readAllBytes(licenseSpecPath));
            }

            if (license == null) {
                return exitCmd(ExitStatus.USAGE, terminal, "no license spec provided");
            }
            return new LicenseGenerator(terminal, publicKeyPath, privateKeyPath, license);
        }

        @Override
        public ExitStatus execute(Settings settings, Environment env) throws Exception {

            // sign
            License license = new LicenseSigner(privateKeyFilePath, publicKeyFilePath).sign(licenseSpec);

            // dump
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
        ExitStatus exitStatus  = new LicenseGeneratorTool().execute(args);
        exit(exitStatus.status());
    }

    @SuppressForbidden(reason = "Allowed to exit explicitly from #main()")
    private static void exit(int status) {
        System.exit(status);
    }
}
