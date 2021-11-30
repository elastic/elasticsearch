/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license.licensor.tools;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.cli.LoggingAwareCommand;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.license.License;
import org.elasticsearch.license.licensor.LicenseSigner;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class LicenseGeneratorTool extends LoggingAwareCommand {

    private final OptionSpec<String> publicKeyPathOption;
    private final OptionSpec<String> privateKeyPathOption;
    private final OptionSpec<String> licenseOption;
    private final OptionSpec<String> licenseFileOption;

    public LicenseGeneratorTool() {
        super("Generates signed elasticsearch license(s) for a given license spec(s)");
        publicKeyPathOption = parser.accepts("publicKeyPath", "path to public key file").withRequiredArg().required();
        privateKeyPathOption = parser.accepts("privateKeyPath", "path to private key file").withRequiredArg().required();
        // TODO: with jopt-simple 5.0, we can make these requiredUnless each other
        // which is effectively "one must be present"
        licenseOption = parser.accepts("license", "license json spec").withRequiredArg();
        licenseFileOption = parser.accepts("licenseFile", "license json spec file").withRequiredArg();
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
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        Path publicKeyPath = parsePath(publicKeyPathOption.value(options));
        Path privateKeyPath = parsePath(privateKeyPathOption.value(options));
        if (Files.exists(privateKeyPath) == false) {
            throw new UserException(ExitCodes.USAGE, privateKeyPath + " does not exist");
        } else if (Files.exists(publicKeyPath) == false) {
            throw new UserException(ExitCodes.USAGE, publicKeyPath + " does not exist");
        }

        final License licenseSpec;
        if (options.has(licenseOption)) {
            final BytesArray bytes = new BytesArray(licenseOption.value(options).getBytes(StandardCharsets.UTF_8));
            licenseSpec = License.fromSource(bytes, XContentType.JSON);
        } else if (options.has(licenseFileOption)) {
            Path licenseSpecPath = parsePath(licenseFileOption.value(options));
            if (Files.exists(licenseSpecPath) == false) {
                throw new UserException(ExitCodes.USAGE, licenseSpecPath + " does not exist");
            }
            final BytesArray bytes = new BytesArray(Files.readAllBytes(licenseSpecPath));
            licenseSpec = License.fromSource(bytes, XContentType.JSON);
        } else {
            throw new UserException(ExitCodes.USAGE, "Must specify either --license or --licenseFile");
        }
        if (licenseSpec == null) {
            throw new UserException(ExitCodes.DATA_ERROR, "Could not parse license spec");
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
        terminal.println(Strings.toString(builder));
    }

    @SuppressForbidden(reason = "Parsing command line path")
    private static Path parsePath(String path) {
        return PathUtils.get(path);
    }

}
