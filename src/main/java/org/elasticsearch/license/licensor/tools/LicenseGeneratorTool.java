/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.licensor.tools;

import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.licensor.ESLicenseSigner;
import org.elasticsearch.license.licensor.LicenseSpecs;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Set;

public class LicenseGeneratorTool {

    static class Options {
        private final Set<ESLicense> licenseSpecs;
        private final String publicKeyFilePath;
        private final String privateKeyFilePath;

        Options(Set<ESLicense> licenseSpecs, String publicKeyFilePath, String privateKeyFilePath) {
            this.licenseSpecs = licenseSpecs;
            this.publicKeyFilePath = publicKeyFilePath;
            this.privateKeyFilePath = privateKeyFilePath;
        }
    }

    private static Options parse(String[] args) throws IOException, ParseException {
        Set<ESLicense> licenseSpecs = new HashSet<>();
        String privateKeyPath = null;
        String publicKeyPath = null;

        for (int i = 0; i < args.length; i++) {
            String command = args[i].trim();
            switch (command) {
                case "--license":
                    String licenseInput = args[++i];
                    licenseSpecs.addAll(LicenseSpecs.fromSource(licenseInput));
                    break;
                case "--licenseFile":
                    File licenseFile = new File(args[++i]);
                    if (licenseFile.exists()) {
                        final byte[] bytes = Files.readAllBytes(Paths.get(licenseFile.getAbsolutePath()));
                        licenseSpecs.addAll(LicenseSpecs.fromSource(bytes));
                    } else {
                        throw new IllegalArgumentException(licenseFile.getAbsolutePath() + " does not exist!");
                    }
                    break;
                case "--publicKeyPath":
                    publicKeyPath = args[++i];
                    break;
                case "--privateKeyPath":
                    privateKeyPath = args[++i];
                    break;
            }
        }

        if (licenseSpecs.size() == 0) {
            throw new IllegalArgumentException("at least one of '--licenses' or '--licenseFile' has to be provided");
        }
        if (publicKeyPath == null) {
            throw new IllegalArgumentException("mandatory option '--publicKeyPath' is missing");
        }
        if (privateKeyPath == null) {
            throw new IllegalArgumentException("mandatory option '--privateKeyPath' is missing");
        }

        return new Options(licenseSpecs, publicKeyPath, privateKeyPath);
    }

    public static void main(String[] args) throws IOException, ParseException {
        run(args, System.out);
    }

    public static void run(String[] args, OutputStream out) throws IOException, ParseException {
        Options options = parse(args);

        ESLicenseSigner signer = new ESLicenseSigner(options.privateKeyFilePath, options.publicKeyFilePath);
        ImmutableSet<ESLicense> signedLicences = signer.sign(options.licenseSpecs);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON, out);

        ESLicenses.toXContent(signedLicences, builder);

        builder.flush();
    }

}
