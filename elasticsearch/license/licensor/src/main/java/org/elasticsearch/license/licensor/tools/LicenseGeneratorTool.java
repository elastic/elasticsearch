/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import java.nio.file.Files;
import java.nio.file.Path;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.elasticsearch.cli.Command;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserError;
import org.elasticsearch.common.cli.Terminal;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.licensor.LicenseSigner;

public class LicenseGeneratorTool extends Command {

    private final OptionSpec<String> publicKeyPathOption;
    private final OptionSpec<String> privateKeyPathOption;
    private final OptionSpec<String> licenseOption;
    private final OptionSpec<String> licenseFileOption;

    public LicenseGeneratorTool() {
        super("Generates signed elasticsearch license(s) for a given license spec(s)");
        publicKeyPathOption = parser.accepts("publicKeyPath", "path to public key file")
            .withRequiredArg().required();
        privateKeyPathOption = parser.accepts("privateKeyPath", "path to private key file")
            .withRequiredArg().required();
        // TODO: with jopt-simple 5.0, we can make these requiredUnless each other
        // which is effectively "one must be present"
        licenseOption = parser.accepts("license", "license json spec")
            .withRequiredArg();
        licenseFileOption = parser.accepts("licenseFile", "license json spec file")
            .withRequiredArg();
    }

    public static void main(String[] args) throws Exception {
        exit(new LicenseGeneratorTool().main(args, Terminal.DEFAULT));
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("This tool generate elasticsearch license(s) for the provided");
        terminal.println("license spec(s). The tool can take arbitrary number of");
        terminal.println("`--license` and/or `--licenseFile` to generate corresponding");
        terminal.println("signed license(s).");
        terminal.println("");
    }

    @Override
    protected int execute(Terminal terminal, OptionSet options) throws Exception {
        Path publicKeyPath = PathUtils.get(publicKeyPathOption.value(options));
        Path privateKeyPath = PathUtils.get(privateKeyPathOption.value(options));
        String licenseSpecString = null;
        if (options.has(licenseOption)) {
            licenseSpecString = licenseOption.value(options);
        }
        Path licenseSpecPath = null;
        if (options.has(licenseFileOption)) {
            licenseSpecPath = PathUtils.get(licenseFileOption.value(options));
        }
        execute(terminal, publicKeyPath, privateKeyPath, licenseSpecString, licenseSpecPath);
        return ExitCodes.OK;
    }

    // pkg private for testing
    void execute(Terminal terminal, Path publicKeyPath, Path privateKeyPath,
                 String licenseSpecString, Path licenseSpecPath) throws Exception {
        if (Files.exists(privateKeyPath) == false) {
            throw new UserError(ExitCodes.USAGE, privateKeyPath + " does not exist");
        } else if (Files.exists(publicKeyPath) == false) {
            throw new UserError(ExitCodes.USAGE, publicKeyPath + " does not exist");
        }

        final License licenseSpec;
        if (licenseSpecString != null) {
            licenseSpec = License.fromSource(licenseSpecString);
        } else if (licenseSpecPath != null) {
            if (Files.exists(licenseSpecPath) == false) {
                throw new UserError(ExitCodes.USAGE, licenseSpecPath + " does not exist");
            }
            licenseSpec = License.fromSource(Files.readAllBytes(licenseSpecPath));
        } else {
            throw new UserError(ExitCodes.USAGE, "Must specify either --license or --licenseFile");
        }

        // sign
        License license = new LicenseSigner(privateKeyPath, publicKeyPath).sign(licenseSpec);

        // dump
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        builder.startObject("license");
        license.toInnerXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        builder.endObject();
        builder.flush();
        terminal.println(builder.string());
    }
}
