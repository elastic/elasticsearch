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
import org.elasticsearch.cli.LoggingAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicenseVerifier;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

public class LicenseVerificationTool extends LoggingAwareCommand {

    private final OptionSpec<String> publicKeyPathOption;
    private final OptionSpec<String> licenseOption;
    private final OptionSpec<String> licenseFileOption;

    public LicenseVerificationTool() {
        super("Generates signed elasticsearch license(s) for a given license spec(s)");
        publicKeyPathOption = parser.accepts("publicKeyPath", "path to public key file")
            .withRequiredArg().required();
        // TODO: with jopt-simple 5.0, we can make these requiredUnless each other
        // which is effectively "one must be present"
        licenseOption = parser.accepts("license", "license json spec")
            .withRequiredArg();
        licenseFileOption = parser.accepts("licenseFile", "license json spec file")
            .withRequiredArg();
    }

    public static void main(String[] args) throws Exception {
        exit(new LicenseVerificationTool().main(args, Terminal.DEFAULT));
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        Path publicKeyPath = parsePath(publicKeyPathOption.value(options));
        if (Files.exists(publicKeyPath) == false) {
            throw new UserException(ExitCodes.USAGE, publicKeyPath + " does not exist");
        }

        final License licenseSpec;
        if (options.has(licenseOption)) {
            final BytesArray bytes =
                    new BytesArray(licenseOption.value(options).getBytes(StandardCharsets.UTF_8));
            licenseSpec =
                    License.fromSource(bytes, XContentType.JSON);
        } else if (options.has(licenseFileOption)) {
            Path licenseSpecPath = parsePath(licenseFileOption.value(options));
            if (Files.exists(licenseSpecPath) == false) {
                throw new UserException(ExitCodes.USAGE, licenseSpecPath + " does not exist");
            }
            final BytesArray bytes = new BytesArray(Files.readAllBytes(licenseSpecPath));
            licenseSpec = License.fromSource(bytes, XContentType.JSON);
        } else {
            throw new UserException(
                    ExitCodes.USAGE,
                    "Must specify either --license or --licenseFile");
        }

        // verify
        if (LicenseVerifier.verifyLicense(licenseSpec, Files.readAllBytes(publicKeyPath)) == false) {
            throw new UserException(ExitCodes.DATA_ERROR, "Invalid License!");
        }
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.startObject();
        builder.startObject("license");
        licenseSpec.toInnerXContent(builder, ToXContent.EMPTY_PARAMS);
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
